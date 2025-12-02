from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import asyncpg
import os
import redis.asyncio as redis
import grpc
from concurrent import futures
try:
    import users_pb2
    import users_pb2_grpc
except ImportError:
    users_pb2 = None
    users_pb2_grpc = None

app = FastAPI()

database_url = os.getenv("DATABASE_URL", "postgresql://crmuser:crmpass@localhost:5432/internalcrm")
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

db_pool = None
redis_client = None

class UserCreate(BaseModel):
    email: str
    name: str
    role: str = "user"

class UserUpdate(BaseModel):
    name: str = None
    role: str = None
    active: bool = None

if users_pb2_grpc:
    class UsersService(users_pb2_grpc.UsersServiceServicer):
    async def GetUser(self, request, context):
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, email, name, role, active, created_at
                FROM users
                WHERE id = $1
            """, request.user_id)
            
            if not row:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("user not found")
                return users_pb2.UserResponse()
            
            return users_pb2.UserResponse(
                id=str(row["id"]),
                email=row["email"],
                name=row["name"],
                role=row["role"],
                active=row["active"],
                created_at=row["created_at"].isoformat()
            )
        
        async def CreateUser(self, request, context):
            async with db_pool.acquire() as conn:
                try:
                    row = await conn.fetchrow("""
                        INSERT INTO users (email, name, role)
                        VALUES ($1, $2, $3)
                        RETURNING id, email, name, role, active, created_at
                    """, request.email, request.name, request.role)
                    
                    return users_pb2.UserResponse(
                        id=str(row["id"]),
                        email=row["email"],
                        name=row["name"],
                        role=row["role"],
                        active=row["active"],
                        created_at=row["created_at"].isoformat()
                    )
                except asyncpg.UniqueViolationError:
                    context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                    context.set_details("user already exists")
                    return users_pb2.UserResponse()
else:
    class UsersService:
        pass

@app.on_event("startup")
async def startup():
    global db_pool, redis_client
    db_pool = await asyncpg.create_pool(database_url)
    redis_client = await redis.from_url(redis_url)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                role VARCHAR(50) DEFAULT 'user',
                active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)
        """)
    
    if users_pb2_grpc:
        server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        users_pb2_grpc.add_UsersServiceServicer_to_server(UsersService(), server)
        server.add_insecure_port('[::]:50051')
        await server.start()

@app.on_event("shutdown")
async def shutdown():
    global db_pool, redis_client
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()

@app.post("/users")
async def create_user(user: UserCreate):
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO users (email, name, role)
                VALUES ($1, $2, $3)
                RETURNING id, email, name, role, active, created_at
            """, user.email, user.name, user.role)
            
            user_data = {
                "id": row["id"],
                "email": row["email"],
                "name": row["name"],
                "role": row["role"],
                "active": row["active"],
                "created_at": row["created_at"].isoformat()
            }
            
            await redis_client.setex(f"user:{row['id']}", 3600, str(row["id"]))
            
            return user_data
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="user already exists")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/{user_id}")
async def get_user(user_id: int):
    try:
        cached = await redis_client.get(f"user:{user_id}")
        if not cached:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT id, email, name, role, active, created_at
                    FROM users
                    WHERE id = $1
                """, user_id)
                
                if row:
                    return {
                        "id": row["id"],
                        "email": row["email"],
                        "name": row["name"],
                        "role": row["role"],
                        "active": row["active"],
                        "created_at": row["created_at"].isoformat()
                    }
        
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, email, name, role, active, created_at
                FROM users
                WHERE id = $1
            """, user_id)
            
            if not row:
                raise HTTPException(status_code=404, detail="user not found")
            
            await redis_client.setex(f"user:{user_id}", 3600, str(user_id))
            
            return {
                "id": row["id"],
                "email": row["email"],
                "name": row["name"],
                "role": row["role"],
                "active": row["active"],
                "created_at": row["created_at"].isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/users/{user_id}")
async def update_user(user_id: int, user_update: UserUpdate):
    try:
        updates = []
        values = []
        param_num = 1
        
        if user_update.name is not None:
            updates.append(f"name = ${param_num}")
            values.append(user_update.name)
            param_num += 1
        
        if user_update.role is not None:
            updates.append(f"role = ${param_num}")
            values.append(user_update.role)
            param_num += 1
        
        if user_update.active is not None:
            updates.append(f"active = ${param_num}")
            values.append(user_update.active)
            param_num += 1
        
        if not updates:
            raise HTTPException(status_code=400, detail="no fields to update")
        
        updates.append(f"updated_at = CURRENT_TIMESTAMP")
        values.append(user_id)
        
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(f"""
                UPDATE users
                SET {', '.join(updates)}
                WHERE id = ${param_num}
                RETURNING id, email, name, role, active, created_at, updated_at
            """, *values)
            
            if not row:
                raise HTTPException(status_code=404, detail="user not found")
            
            await redis_client.delete(f"user:{user_id}")
            
            return {
                "id": row["id"],
                "email": row["email"],
                "name": row["name"],
                "role": row["role"],
                "active": row["active"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/users/{user_id}")
async def delete_user(user_id: int):
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM users
                WHERE id = $1
            """, user_id)
            
            if result == "DELETE 0":
                raise HTTPException(status_code=404, detail="user not found")
            
            await redis_client.delete(f"user:{user_id}")
            
            return {"status": "ok", "message": "user deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users")
async def list_users(limit: int = 100, offset: int = 0):
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, email, name, role, active, created_at
                FROM users
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
            """, limit, offset)
            
            result = []
            for row in rows:
                result.append({
                    "id": row["id"],
                    "email": row["email"],
                    "name": row["name"],
                    "role": row["role"],
                    "active": row["active"],
                    "created_at": row["created_at"].isoformat()
                })
            
            return {"users": result, "count": len(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "ok"}

