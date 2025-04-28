# Projeto de Tratamento de Dados Públicos de Síndrome Respiratória Aguda Grave (SRAG)

Este projeto implementa um data lakehouse para processar e analisar dados públicos de Síndrome Respiratória Aguda Grave (SRAG) disponibilizados pelo portal dados.gov.br. O projeto utiliza Apache Airflow para orquestração de dados e PySpark para processamento distribuído.

## Arquitetura

O projeto segue uma arquitetura de lakehouse com as seguintes camadas:
- **Bronze**: Dados brutos originais
- **Silver**: Dados limpos e padronizados
- **Gold**: Dados agregados e prontos para análise

## Pré-requisitos

- Docker e Docker Compose
- Python 3.8+
- Mínimo de 4GB de RAM
- Mínimo de 2 CPUs
- Mínimo de 10GB de espaço em disco

## Configuração do Ambiente

1. **Clone o repositório**
   ```bash
   git clone https://github.com/SR-Dionizio/lakehouse_srag
   cd lakehouse_srag
   ```

2. **Baixe os dados de origem**
   - Acesse: [SRAG 2021 e 2022](https://dados.gov.br/dados/conjuntos-dados/srag-2021-e-2022)
   - Faça o download dos arquivos CSV
   - Coloque os arquivos na pasta `data/bronze/`

3. **Configuração do ambiente Docker**
   ```bash
   # Construa as imagens Docker
   docker-compose build
   
   # Inicie os serviços
   docker-compose up -d
   ```

## Estrutura do Projeto

```
lakehouse_srag/
├── dags/ # DAGs do Airflow
├── scripts/ # Scripts de processamento
├── data/ # Diretórios de dados
│ ├── bronze/ # Dados brutos
│ ├── silver/ # Dados processados
│ └── gold/ # Dados agregados
├── config/ # Arquivos de configuração
├── plugins/ # Plugins do Airflow
├── logs/ # Logs do sistema
└── docs/ # Documentação adicional
```

## Principais Componentes

- **Apache Airflow**: Orquestração de pipelines de dados (acessível em http://localhost:8080)
- **Apache Spark**: Processamento distribuído de dados
- **PostgreSQL**: Banco de dados para metadados do Airflow
- **Redis**: Message broker para o Airflow

## Como Usar

1. **Acesse o Airflow**
   - URL: http://localhost:8080
   - Usuário padrão: airflow
   - Senha padrão: airflow

2. **Ative as DAGs**
   - No painel do Airflow, ative as DAGs necessárias
   - As DAGs processarão automaticamente os dados seguindo o fluxo bronze → silver → gold

## Dependências Principais

- PySpark 3.5.4
- Python 3.8+
- Outras dependências listadas em `requirements.txt`

## Monitoramento

- Logs do Airflow disponíveis em `logs/`
- Interface web do Airflow para monitoramento de DAGs
- Healthchecks configurados para todos os serviços

## Troubleshooting

1. **Problemas de Permissão**
   ```bash
   # Ajuste as permissões se necessário
   sudo chown -R 50000:0 logs/ plugins/ dags/
   ```

2. **Serviços não Iniciam**
   ```bash
   # Verifique os logs
   docker-compose logs -f
   ```

3. **Problemas de Recursos**
   - Verifique se seu sistema atende aos requisitos mínimos
   - Ajuste as configurações de recursos no Docker conforme necessário

