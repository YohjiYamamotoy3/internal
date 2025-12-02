from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import asyncpg
import os
import redis.asyncio as redis
import grpc
from concurrent import futures
try:
    import payments_pb2
    import payments_pb2_grpc
except ImportError:
    payments_pb2 = None
    payments_pb2_grpc = None

app = FastAPI()

database_url = os.getenv("DATABASE_URL", "postgresql://crmuser:crmpass@localhost:5432/internalcrm")
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

db_pool = None
redis_client = None

class PaymentCreate(BaseModel):
    user_id: int
    amount: float
    currency: str = "USD"
    description: str = None

if payments_pb2_grpc:
    class PaymentsService(payments_pb2_grpc.PaymentsServiceServicer):
        async def CreatePayment(self, request, context):
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    INSERT INTO payments (user_id, amount, currency, description, status)
                    VALUES ($1, $2, $3, $4, 'pending')
                    RETURNING id, user_id, amount, currency, description, status, created_at
                """, request.user_id, request.amount, request.currency, request.description)
                
                return payments_pb2.PaymentResponse(
                    id=str(row["id"]),
                    user_id=row["user_id"],
                    amount=row["amount"],
                    currency=row["currency"],
                    status=row["status"],
                    created_at=row["created_at"].isoformat()
                )
        
        async def GetPayment(self, request, context):
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT id, user_id, amount, currency, description, status, created_at
                    FROM payments
                    WHERE id = $1
                """, request.payment_id)
                
                if not row:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details("payment not found")
                    return payments_pb2.PaymentResponse()
                
                return payments_pb2.PaymentResponse(
                    id=str(row["id"]),
                    user_id=row["user_id"],
                    amount=row["amount"],
                    currency=row["currency"],
                    status=row["status"],
                    created_at=row["created_at"].isoformat()
                )
else:
    class PaymentsService:
        pass

@app.on_event("startup")
async def startup():
    global db_pool, redis_client
    db_pool = await asyncpg.create_pool(database_url)
    redis_client = await redis.from_url(redis_url)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount DECIMAL(10, 2) NOT NULL,
                currency VARCHAR(10) DEFAULT 'USD',
                description TEXT,
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status)
        """)
    
    if payments_pb2_grpc:
        server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        payments_pb2_grpc.add_PaymentsServiceServicer_to_server(PaymentsService(), server)
        server.add_insecure_port('[::]:50052')
        await server.start()

@app.on_event("shutdown")
async def shutdown():
    global db_pool, redis_client
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()

@app.post("/payments")
async def create_payment(payment: PaymentCreate):
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO payments (user_id, amount, currency, description, status)
                VALUES ($1, $2, $3, $4, 'pending')
                RETURNING id, user_id, amount, currency, description, status, created_at
            """, payment.user_id, payment.amount, payment.currency, payment.description)
            
            payment_data = {
                "id": row["id"],
                "user_id": row["user_id"],
                "amount": float(row["amount"]),
                "currency": row["currency"],
                "description": row["description"],
                "status": row["status"],
                "created_at": row["created_at"].isoformat()
            }
            
            await redis_client.lpush("payment_queue", str(row["id"]))
            
            return payment_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/payments/{payment_id}")
async def get_payment(payment_id: int):
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, user_id, amount, currency, description, status, created_at
                FROM payments
                WHERE id = $1
            """, payment_id)
            
            if not row:
                raise HTTPException(status_code=404, detail="payment not found")
            
            return {
                "id": row["id"],
                "user_id": row["user_id"],
                "amount": float(row["amount"]),
                "currency": row["currency"],
                "description": row["description"],
                "status": row["status"],
                "created_at": row["created_at"].isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/payments")
async def list_payments(user_id: int = None, limit: int = 100, offset: int = 0):
    try:
        async with db_pool.acquire() as conn:
            if user_id:
                rows = await conn.fetch("""
                    SELECT id, user_id, amount, currency, description, status, created_at
                    FROM payments
                    WHERE user_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                """, user_id, limit, offset)
            else:
                rows = await conn.fetch("""
                    SELECT id, user_id, amount, currency, description, status, created_at
                    FROM payments
                    ORDER BY created_at DESC
                    LIMIT $1 OFFSET $2
                """, limit, offset)
            
            result = []
            for row in rows:
                result.append({
                    "id": row["id"],
                    "user_id": row["user_id"],
                    "amount": float(row["amount"]),
                    "currency": row["currency"],
                    "description": row["description"],
                    "status": row["status"],
                    "created_at": row["created_at"].isoformat()
                })
            
            return {"payments": result, "count": len(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/payments/{payment_id}/status")
async def update_payment_status(payment_id: int, status: str):
    try:
        if status not in ["pending", "completed", "failed", "cancelled"]:
            raise HTTPException(status_code=400, detail="invalid status")
        
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                UPDATE payments
                SET status = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = $2
                RETURNING id, user_id, amount, currency, description, status, created_at, updated_at
            """, status, payment_id)
            
            if not row:
                raise HTTPException(status_code=404, detail="payment not found")
            
            return {
                "id": row["id"],
                "user_id": row["user_id"],
                "amount": float(row["amount"]),
                "currency": row["currency"],
                "description": row["description"],
                "status": row["status"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "ok"}

