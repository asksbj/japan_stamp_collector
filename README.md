# japan_stamp_collector

日本郵便 風景印 爬虫与数据迁移。

## 迁移数据到 CloudBase

将 `dist/` 下各省的图片与 `data.json` 同步到 CloudBase 云存储与文档库：

1. **环境变量**（必填）  
   - `TCB_ENV_ID`：CloudBase 环境 ID  
   - `TCB_ACCESS_TOKEN`：云存储 HTTP API 用 Token（控制台「身份认证」-「Token 管理」）  
   - `TCB_SECRET_ID` / `TCB_SECRET_KEY`：腾讯云密钥，供 Node 脚本以管理员身份写入文档库  

2. **安装依赖**  
   - Python：`pip install -r pip-requirements`（含 `requests`）  
   - Node：在项目根目录执行 `npm install`（用于写入 JPostFuke 集合）  

3. **执行迁移**  
   - 在项目根目录执行：`python3 etl/cloudbase_migrate.py`  
   - 脚本会：  
     - 将 `dist/<省英文名>/images/` 下文件上传到云存储 `japan_collectorsjpost_fukes/<省英文名>/`  
     - 将 `dist/<省英文名>/data.json` 中每条记录写入文档库集合 `JPostFuke`，并把 `image` 字段改为对应文件的云存储地址（`cloudObjectId`）