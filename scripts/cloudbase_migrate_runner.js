/**
 * 将 cloudbase_migrate.py 生成的文档 JSON 批量写入 CloudBase 文档数据库 JPostFuke。
 * 需设置环境变量: TCB_ENV_ID, TCB_SECRET_ID, TCB_SECRET_KEY
 * 用法: node etl/cloudbase_migrate_runner.js <path-to-docs.json>
 */

const fs = require('fs');
const path = require('path');

const envId = process.env.TCB_ENV_ID;
const secretId = process.env.TCB_SECRET_ID;
const secretKey = process.env.TCB_SECRET_KEY;

if (!envId || !secretId || !secretKey) {
  console.error('缺少环境变量: TCB_ENV_ID, TCB_SECRET_ID, TCB_SECRET_KEY');
  process.exit(1);
}

const docsPath = process.argv[2];
if (!docsPath || !fs.existsSync(docsPath)) {
  console.error('用法: node etl/cloudbase_migrate_runner.js <path-to-docs.json>');
  process.exit(1);
}

const COLLECTION = 'JPostFuke';
const BATCH = 100;

async function main() {
  const cloudbase = require('@cloudbase/node-sdk');
  const app = cloudbase.init({
    env: envId,
    secretId,
    secretKey,
  });
  const db = app.database();

  const raw = fs.readFileSync(docsPath, 'utf8');
  const docs = JSON.parse(raw);
  if (!Array.isArray(docs) || docs.length === 0) {
    console.log('文档列表为空，跳过写入');
    return;
  }

  const collection = db.collection(COLLECTION);
  let total = 0;
  for (let i = 0; i < docs.length; i += BATCH) {
    const batch = docs.slice(i, i + BATCH);
    // 部分环境 add 仅支持单条，逐条写入
    for (const doc of batch) {
      await collection.add(doc);
      total += 1;
    }
    console.log(`已写入 ${total}/${docs.length} 条`);
  }
  console.log(`JPostFuke 写入完成，共 ${total} 条`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
