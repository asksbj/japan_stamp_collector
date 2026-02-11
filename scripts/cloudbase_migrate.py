#!/usr/bin/env python3
"""
将 dist 下的风景印数据迁移到 CloudBase：
1. 各省 images/ 内图片上传到云存储 japan_collectorsjpost_fukes/{省英文名}/
2. 各省 data.json 中每条记录写入文档数据库 JPostFuke，image 字段改为云存储位置（cloudObjectId）

环境变量（必填）：
  TCB_ENV_ID          CloudBase 环境 ID
  TCB_ACCESS_TOKEN    用于云存储 HTTP API 的 Access Token（控制台「身份认证」-「Token 管理」获取）
  TCB_SECRET_ID       腾讯云 SecretId（Node 脚本写库用，需具备数据库写权限）
  TCB_SECRET_KEY      腾讯云 SecretKey

依赖：pip install requests
文档库写入依赖 Node：在项目根目录执行 npm install 后，本脚本会调用 etl/cloudbase_migrate_runner.js 写入 JPostFuke。
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import requests

# 与 base_crawler 保持一致
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DIST_DIR = PROJECT_ROOT / "dist"

STORAGE_PREFIX = "japan_collectorsjpost_fukes"
COLLECTION_NAME = "JPostFuke"
BATCH_UPLOAD_INFO = 20
BATCH_INSERT = 100


def env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        print(f"错误: 未设置环境变量 {name}", file=sys.stderr)
        sys.exit(1)
    return v.strip()


def get_base_url(env_id: str) -> str:
    return f"https://{env_id}.api.tcloudbasegateway.com"


def upload_images_for_prefecture(
    base_url: str,
    token: str,
    prefecture: str,
    images_dir: Path,
) -> dict[str, str]:
    """
    将 images_dir 下所有文件上传到云存储 japan_collectorsjpost_fukes/{prefecture}/
    返回: { 本地文件名: cloudObjectId }
    """
    image_files = [f for f in images_dir.iterdir() if f.is_file()]
    if not image_files:
        return {}

    filename_to_cloud: dict[str, str] = {}
    headers_api = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    for i in range(0, len(image_files), BATCH_UPLOAD_INFO):
        batch = image_files[i : i + BATCH_UPLOAD_INFO]
        object_ids = [
            {"objectId": f"{STORAGE_PREFIX}/{prefecture}/{f.name}"}
            for f in batch
        ]
        resp = requests.post(
            f"{base_url}/v1/storages/get-objects-upload-info",
            headers=headers_api,
            json=object_ids,
            timeout=30,
        )
        resp.raise_for_status()
        results = resp.json()
        if not isinstance(results, list) or len(results) != len(batch):
            raise RuntimeError(
                f"get-objects-upload-info 返回数量与请求不一致: {len(results)} vs {len(batch)}"
            )

        for idx, (local_path, item) in enumerate(zip(batch, results)):
            if "code" in item:
                print(
                    f"  警告: {local_path.name} 获取上传信息失败: {item.get('message', item)}",
                    file=sys.stderr,
                )
                continue
            upload_url = item.get("uploadUrl")
            auth = item.get("authorization")
            cos_token = item.get("token")
            meta = item.get("cloudObjectMeta")
            cloud_object_id = item.get("cloudObjectId")
            if not all([upload_url, auth, cloud_object_id]):
                print(
                    f"  警告: {local_path.name} 返回缺少 uploadUrl/authorization/cloudObjectId",
                    file=sys.stderr,
                )
                continue

            with open(local_path, "rb") as f:
                body = f.read()

            put_headers = {
                "Authorization": auth,
                "X-Cos-Security-Token": cos_token or "",
                "X-Cos-Meta-Fileid": meta or "",
            }
            put_resp = requests.put(
                upload_url,
                headers=put_headers,
                data=body,
                timeout=60,
            )
            if put_resp.status_code >= 400:
                print(
                    f"  警告: 上传失败 {local_path.name} HTTP {put_resp.status_code}",
                    file=sys.stderr,
                )
                continue
            filename_to_cloud[local_path.name] = cloud_object_id

    return filename_to_cloud


def load_docs_and_rewrite_images(
    data_path: Path,
    filename_to_cloud: dict[str, str],
) -> list[dict]:
    """加载 data.json，将每条记录的 image 替换为 cloudObjectId。"""
    with open(data_path, "r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        records = [records]
    out = []
    for rec in records:
        rec = dict(rec)
        img = rec.get("image")
        if img and isinstance(img, str) and img in filename_to_cloud:
            rec["image"] = filename_to_cloud[img]
        out.append(rec)
    return out


def run_node_runner(docs_path: Path) -> None:
    """调用 Node 脚本将 docs_path 中的文档写入 JPostFuke。"""
    runner = SCRIPT_DIR / "cloudbase_migrate_runner.js"
    if not runner.exists():
        print(
            "未找到 etl/cloudbase_migrate_runner.js，跳过写入文档库。"
            " 可将生成的文档 JSON 在控制台手动导入，或配置 Node 环境后重新运行。",
            file=sys.stderr,
        )
        return
    node = subprocess.run(
        [os.environ.get("NODE", "node"), str(runner), str(docs_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    if node.returncode != 0:
        print("Node 写入脚本执行失败:", file=sys.stderr)
        if node.stderr:
            print(node.stderr, file=sys.stderr)
        sys.exit(node.returncode)
    if node.stdout:
        print(node.stdout.rstrip())


def main() -> None:
    env_id = env("TCB_ENV_ID")
    token = env("TCB_ACCESS_TOKEN")
    base_url = get_base_url(env_id)

    if not DIST_DIR.exists():
        print(f"错误: dist 目录不存在 {DIST_DIR}", file=sys.stderr)
        sys.exit(1)

    all_docs: list[dict] = []
    prefecture_dirs = sorted(
        d
        for d in DIST_DIR.iterdir()
        if d.is_dir()
        and (d / "data.json").exists()
        and (d / "images").is_dir()
    )

    if not prefecture_dirs:
        print("未找到同时含有 data.json 和 images/ 的省份目录", file=sys.stderr)
        sys.exit(1)

    for prefecture_dir in prefecture_dirs:
        prefecture = prefecture_dir.name
        images_dir = prefecture_dir / "images"
        data_path = prefecture_dir / "data.json"
        print(f"[{prefecture}] 上传图片...")
        filename_to_cloud = upload_images_for_prefecture(
            base_url, token, prefecture, images_dir
        )
        print(f"  上传成功: {len(filename_to_cloud)} 个文件")
        print(f"[{prefecture}] 加载 data.json 并替换 image 为云存储地址...")
        docs = load_docs_and_rewrite_images(data_path, filename_to_cloud)
        all_docs.extend(docs)
        print(f"  文档数: {len(docs)}")

    if not all_docs:
        print("没有可写入的文档")
        return

    docs_file = SCRIPT_DIR / "cloudbase_migrate_docs.json"
    with open(docs_file, "w", encoding="utf-8") as f:
        json.dump(all_docs, f, ensure_ascii=False, indent=2)
    print(f"已生成文档 JSON: {docs_file}（共 {len(all_docs)} 条）")
    print("正在调用 Node 脚本写入 JPostFuke...")
    run_node_runner(docs_file)
    print("迁移完成。")


if __name__ == "__main__":
    main()
