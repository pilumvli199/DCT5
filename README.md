# DhanHQ Commodity Price Telegram Bot

Automated bot that sends MCX commodity prices to Telegram every minute.

## Setup on Railway.app

1. **Fork/Upload this code to GitHub**

2. **Create Railway Account**
   - Visit https://railway.app
   - Sign up with GitHub

3. **Create New Project**
   - Click "New Project"
   - Select "Deploy from GitHub repo"
   - Choose your repository

4. **Add Environment Variables**
   Go to Variables tab and add:
   ```
   DHAN_CLIENT_ID=your_dhan_client_id
   DHAN_ACCESS_TOKEN=your_dhan_access_token
   TELEGRAM_BOT_TOKEN=your_telegram_bot_token
   TELEGRAM_CHAT_ID=your_telegram_chat_id
   ```

5. **Deploy**
   - Railway will automatically detect Python and deploy
   - Check logs to ensure bot is running

## Getting Required Credentials

### DhanHQ:
1. Visit https://dhanhq.co/
2. Create account and complete KYC
3. Go to Developer Console
4. Generate API credentials

### Telegram Bot:
1. Open Telegram and search for @BotFather
2. Send `/newbot` command
3. Follow instructions to create bot
4. Copy the Bot Token

### Telegram Chat ID:
1. Send a message to your bot
2. Visit: `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`
3. Find your chat ID in the response

## Commodities Tracked

- Gold
- Silver
- Crude Oil
- Natural Gas
- Copper

## Features

- ✅ Real-time LTP updates every minute
- ✅ Price change indicators (📈📉)
- ✅ Automatic reconnection on errors
- ✅ Logging for monitoring
- ✅ Railway.app compatible

## Troubleshooting

Check Railway logs if bot doesn't start:
```bash
railway logs
```

Common issues:
- Missing environment variables
- Invalid API credentials
- Wrong Chat ID format
