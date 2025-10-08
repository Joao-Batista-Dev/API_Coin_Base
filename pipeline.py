import requests, time, os, logging
import logfire
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, BitcoinPreco
from dotenv import load_dotenv
from logging import basicConfig, getLogger


load_dotenv()


# configuração logfire
logfire.configure(token=os.environ.get('LOGGER_KEY'))
basicConfig(handlers=[logfire.LogfireLoggingHandler()])
logger = getLogger(__name__)
logger.setLevel(logging.INFO)
logfire.instrument_requests()
logfire.instrument_sqlalchemy()


# configuração database
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
    logger.info('Tabela criada/verificada com sucesso!')

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

    logger.info(f'[{dados['timestamp']}] Dados salvos no PostgreSQL!')


if __name__ == "__main__":
    criar_tabela()
    logger.info('Iniciando ETL a cada 15 segundos')

    while True:
        try: 
            dados_json = extract_dados_bitcoin()
            if dados_json:
                dados_tratados = transform_dados_bitcoin(dados_json)
                logger.info('Dados tratados', dados_tratados)
                salvar_dados_postgres(dados_tratados)
            time.sleep(15)  
        except KeyboardInterrupt:
            logger.info('Precesso interropido!')
    