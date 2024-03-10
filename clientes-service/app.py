from fastapi import FastAPI, Query, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, func, Column, Float, Integer, String, Text, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from aioredis import create_redis_pool
import random, lorem, json
import enum

class transactionType(enum.Enum):
    C = 'c'
    D = 'd'

app = FastAPI()

# Database configuration
database_url = 'postgresql://postgres:password@db:5432/clientes_db'
engine = create_engine(database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Redis caching configuration
redis_url = 'redis://caching-db:6379/0'
redis = None

# Dependency to get a database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Product model
class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True)
    price = Column(Float)
    description = Column(String(500))

class Extract(Base):
    __tablename__ = 'extrato'

    id = Column(Integer, primary_key=True)
    saldo =  Column(Integer)
    data_extrato = Column(Date)
    limit = Column(Integer)
    client = Column(Integer)

class Transaction(BaseModel):
    valor:  int
    tipo: transactionType
    descricao: str

class Cliente(Base):
    __tablename__ = 'cliente'

    id_cliente = Column(Integer, primary_key=True)
    nome_cliente = Column(String(100), unique=True)

# Create database tables
Base.metadata.create_all(bind=engine)

# Set up Redis connection
@app.on_event("startup")
async def startup_event():
    global redis
    redis = await create_redis_pool(redis_url)

# Shutdown Redis connection
@app.on_event("shutdown")
async def shutdown_event():
    global redis
    redis.close()
    await redis.wait_closed()

# API endpoints

@app.get('/clientes')
async def get_all_clients(db = Depends(get_db)):
    clientes = await redis.get('cliente')
    if clientes:
        return json.loads(clientes)
    
    clientes = db.query(Cliente).all()
    result = []
    for cliente in clientes:
        result.append({
            'name': cliente.nome_cliente,
        })
    await redis.set('clients', json.dumps(result))

    return result, 200

@app.post("/clientes/{id}/transacoes")
async def create_item(id:int, transaction: Transaction):
    transcation_dict = {"CLIENTE_TRANSACAO": id, "SALDO_D": transaction.valor, "DESCRICAO_D": transaction.descricao}
    db = Depends(get_db)
    
    if(transaction.tipo is transactionType.D):
        db.execute('CALL DEBITO(CLIENTE_TRANSACAO, SALDO_D, DESCRICAO_D)', transcation_dict)
    # puxar a proc
    # mandar as coisas p proc
    transaction_response = {"limite": transaction.valor, "saldo": transaction.tipo }
    return transaction_response


@app.get('/clientes/{id}/extrato')
async def get_extract(id_cliente: int, extrato = Extract, db = Depends(get_db)):
    
    extratos = db.query(Extract).filter(Extract.id == id).all()
    #transactions = db.query(Transaction).filter(Transaction.id == id).all()
    result = []

    for extrato in extratos:
        result.append({
            'saldo': extrato.saldo,
            'limite': extrato.limite,
            'data_extrato': extrato.data_extrato
        })
    '''
    result.append("ultimas_transacoes:")

    for transaction in transactions:
        result.append({
            'valor': transaction.valor,
            'tipo': transaction.tipo,
        })
    '''
    return result, 200





'''clientes = await redis.get('cliente')
    if clientes:
        return json.loads(clientes)
    
    clientes = db.query(Cliente).all()
    result = []
    for cliente in clientes:
        result.append({
            'name': cliente.nome_cliente,
        })
    await redis.set('clients', json.dumps(result))

    return result, 200'''

@app.get('/products/{name}')
async def get_product_by_name(name: str, db = Depends(get_db)):
    product = await redis.get(name)
    if product:
        return json.loads(product)

    product = db.query(Product).filter_by(name=name).first()
    if product:
        result = {
            'name': product.name,
            'price': product.price,
            'description': product.description
            }
        await redis.set(name, json.dumps(result))
        return result
    else:
        raise HTTPException(status_code=404, detail='Product not found')