# Binance Data Pipeline

A data pipeline for collecting and analyzing Binance cryptocurrency data, including spot and futures market data, funding rates, and margin interest rates.

## Features

- Collects spot and perpetual futures market data from Binance
- Stores data in DuckDB for efficient querying
- Automated data updates on configurable schedules
- Telegram notifications for important events
- Docker support for easy deployment

## Prerequisites

- Git
- Docker and Docker Compose
- Binance API credentials
- Telegram Bot Token (optional)

## Installation & Deployment

### 1. Clone the Repository

```bash
cd /data/max # or your preferred directory
git clone https://github.com/albusmaxgrangerthu/binancedatapipeline.git
cd binancedatapipeline
```

### 2. Configure Environment Variables

Create a `.env` file in the project root:

```bash
## Create and edit .env file
cat > .env << EOL
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret
COINGECKO_API_KEY=your_coingecko_api_key

DATABASE_DIR=/app/data
CODEBOOK_DIR=/app/codebook

TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
EOL
```

### 3. Create Required Directories

```bash
mkdir -p data logs
```

### 4. Build and Run with Docker

```bash
docker-compose build # build the container
docker-compose up -d # run in detached mode
docker-compose logs -f # view logs
```

## Project Structure

```bash
binancedatapipeline/
├── app/
│ └── src/
│ ├── scheduler.py # Main scheduling script
│ ├── crypto_data_pipeline.py
│ └── crypto_utils.py
├── data/ # Database storage
├── logs/ # Application logs
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env # Configuration (not in git)
```

## Updating the Application

To update the application with the latest changes:

```bash
git pull # pull the latest changes
docker-compose down # Rebuild and restart containers
docker-compose up -d --build
```

## Monitoring

### View Container Status

```bash
docker-compose ps
```

### Check Logs

```bash
docker-compose logs -f
```

## Data Storage

- All market data is stored in DuckDB database in the `data` directory
- Logs are stored in the `logs` directory
- Both directories are mounted as volumes in the Docker container

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details

## Acknowledgments

- Binance API
- DuckDB
- Python community

## Notes

1. Need to use binance api key and binance api secret, fill it in the .env
2. can first try manually update in the .ipynb file and see if it works
3. the freq of kilnes are 1min, might have too many data and took too much time to update if the time range are too large
