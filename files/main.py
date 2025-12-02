from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
import os
import aiofiles
from datetime import datetime
from pathlib import Path
import asyncpg
import redis.asyncio as redis
import grpc
from concurrent import futures
try:
    import files_pb2
    import files_pb2_grpc
except ImportError:
    files_pb2 = None
    files_pb2_grpc = None

app = FastAPI()

database_url = os.getenv("DATABASE_URL", "postgresql://crmuser:crmpass@localhost:5432/internalcrm")
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
files_dir = os.getenv("FILES_DIR", "/app/storage")

Path(files_dir).mkdir(parents=True, exist_ok=True)

db_pool = None
redis_client = None

if files_pb2_grpc:
    class FilesService(files_pb2_grpc.FilesServiceServicer):
        async def GetFile(self, request, context):
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT id, filename, path, size, user_id, created_at
                    FROM files
                    WHERE id = $1
                """, request.file_id)
                
                if not row:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details("file not found")
                    return files_pb2.FileResponse()
                
                return files_pb2.FileResponse(
                    id=str(row["id"]),
                    filename=row["filename"],
                    path=row["path"],
                    size=row["size"],
                    user_id=row["user_id"],
                    created_at=row["created_at"].isoformat()
                )
else:
    class FilesService:
        pass

@app.on_event("startup")
async def startup():
    global db_pool, redis_client
    db_pool = await asyncpg.create_pool(database_url)
    redis_client = await redis.from_url(redis_url)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                path VARCHAR(500) NOT NULL,
                size BIGINT NOT NULL,
                user_id INTEGER,
                mime_type VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_user ON files(user_id)
        """)
    
    if files_pb2_grpc:
        server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        files_pb2_grpc.add_FilesServiceServicer_to_server(FilesService(), server)
        server.add_insecure_port('[::]:50054')
        await server.start()

@app.on_event("shutdown")
async def shutdown():
    global db_pool, redis_client
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()

@app.post("/files/upload")
async def upload_file(file: UploadFile = File(...), user_id: int = None):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="no filename")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = f"{timestamp}_{file.filename}"
        filepath = os.path.join(files_dir, safe_filename)
        
        size = 0
        async with aiofiles.open(filepath, 'wb') as f:
            content = await file.read()
            size = len(content)
            await f.write(content)
        
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO files (filename, path, size, user_id, mime_type)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id, filename, path, size, user_id, mime_type, created_at
            """, file.filename, safe_filename, size, user_id, file.content_type)
            
            file_data = {
                "id": row["id"],
                "filename": row["filename"],
                "path": row["path"],
                "size": row["size"],
                "user_id": row["user_id"],
                "mime_type": row["mime_type"],
                "created_at": row["created_at"].isoformat()
            }
            
            await redis_client.setex(f"file:{row['id']}", 3600, str(row["id"]))
            
            return file_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/{file_id}")
async def get_file_info(file_id: int):
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, filename, path, size, user_id, mime_type, created_at
                FROM files
                WHERE id = $1
            """, file_id)
            
            if not row:
                raise HTTPException(status_code=404, detail="file not found")
            
            return {
                "id": row["id"],
                "filename": row["filename"],
                "path": row["path"],
                "size": row["size"],
                "user_id": row["user_id"],
                "mime_type": row["mime_type"],
                "created_at": row["created_at"].isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/{file_id}/download")
async def download_file(file_id: int):
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT filename, path
                FROM files
                WHERE id = $1
            """, file_id)
            
            if not row:
                raise HTTPException(status_code=404, detail="file not found")
            
            filepath = os.path.join(files_dir, row["path"])
            
            if not os.path.exists(filepath):
                raise HTTPException(status_code=404, detail="file not found on disk")
            
            return FileResponse(
                filepath,
                media_type="application/octet-stream",
                filename=row["filename"]
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files")
async def list_files(user_id: int = None, limit: int = 100, offset: int = 0):
    try:
        async with db_pool.acquire() as conn:
            if user_id:
                rows = await conn.fetch("""
                    SELECT id, filename, path, size, user_id, mime_type, created_at
                    FROM files
                    WHERE user_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                """, user_id, limit, offset)
            else:
                rows = await conn.fetch("""
                    SELECT id, filename, path, size, user_id, mime_type, created_at
                    FROM files
                    ORDER BY created_at DESC
                    LIMIT $1 OFFSET $2
                """, limit, offset)
            
            result = []
            for row in rows:
                result.append({
                    "id": row["id"],
                    "filename": row["filename"],
                    "path": row["path"],
                    "size": row["size"],
                    "user_id": row["user_id"],
                    "mime_type": row["mime_type"],
                    "created_at": row["created_at"].isoformat()
                })
            
            return {"files": result, "count": len(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/files/{file_id}")
async def delete_file(file_id: int):
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT path
                FROM files
                WHERE id = $1
            """, file_id)
            
            if not row:
                raise HTTPException(status_code=404, detail="file not found")
            
            filepath = os.path.join(files_dir, row["path"])
            
            await conn.execute("""
                DELETE FROM files
                WHERE id = $1
            """, file_id)
            
            if os.path.exists(filepath):
                os.remove(filepath)
            
            await redis_client.delete(f"file:{file_id}")
            
            return {"status": "ok", "message": "file deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "ok"}
