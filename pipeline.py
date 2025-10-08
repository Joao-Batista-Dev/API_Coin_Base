import requests, time, os
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, BitcoinPreco
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DBNAME=os.environ.get('POSTGRES_DBNAME')
POSTGRES_USER=os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD=os.environ.get('POSTGRES_PASSWORD')
POSTGRES_HOST=os.environ.get('POSTGRES_HOST')
POSTGRES_PORT=os.environ.get('POSTGRES_PORT')

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DBNAME}"
)

# criar o engine da sessão
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def criar_tabela():
    """Criar tabela no banco de dados, se não existir."""
    Base.metadata.create_all(engine)
    print('Tabela criada/verificada com sucesso!')

def extract_dados_bitcoin():
    url = 'https://api.coinbase.com/v2/prices/spot'

    response = requests.get(url)
    dados = response.json()

    return dados


def transform_dados_bitcoin(dados):
    valor = dados['data']['amount']
    criptomoeda = dados['data']['base']
    moeda = dados['data']['currency']
    timestamp= datetime.now()

    dados_transformado = {
        'valor': valor,
        'criptomoeda': criptomoeda,
        'moeda': moeda,
        'timestamp': timestamp,
    }

    return dados_transformado


def salvar_dados_postgres(dados):
    """Salvar dados no banco PostgreSQL"""
    session = Session()
    novo_registro = BitcoinPreco(**dados)
    session.add(novo_registro)
    session.commit()
    session.close()

    print(f'[{dados['timestamp']}] Dados salvos no PostgreSQL!')


if __name__ == "__main__":
    criar_tabela()
    print('Iniciando ETL a cada 15 segundos')

    while True:
        try: 
            dados_json = extract_dados_bitcoin()
            if dados_json:
                dados_tratados = transform_dados_bitcoin(dados_json)
                print('Dados tratados', dados_tratados)
                salvar_dados_postgres(dados_tratados)
            time.sleep(15)  
        except KeyboardInterrupt:
            print('Precesso interropido!')
    