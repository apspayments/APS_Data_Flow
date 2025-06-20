# GitHub Actions Financial Data Pipeline

Automated financial data processing pipeline that runs on GitHub Actions, processing CSV attachments from Gmail and uploading to Supabase.

## Features

- **Email Processing**: Automatically searches for transaction emails with CSV attachments
- **Data Cleaning**: Standardizes financial data and datetime formats
- **Database Upload**: Bulk uploads to Supabase with retry logic and chunking
- **Error Handling**: Comprehensive logging and timeout management
- **Notifications**: Email notifications for processing status
- **GitHub Actions**: Automated daily runs with artifact storage

## Architecture

```
Gmail � CSV Attachments � Data Processing � Supabase Database
                     �
              GitHub Actions Pipeline
```

## Setup

### 1. Dependencies

```bash
pip install -r requirements.txt
```

### 2. GitHub Secrets

Add these secrets to your GitHub repository settings:

- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_KEY`: Your Supabase service role key
- `BUCKET_NAME`: Supabase storage bucket name
- `FILE_PATH`: Path to XML configuration file in storage
- `REFRESH_TOKEN`: Gmail OAuth refresh token
- `CLIENT_ID`: Google OAuth client ID
- `CLIENT_SECRET`: Google OAuth client secret
- `TOKEN_URI`: Google OAuth token URI (usually `https://oauth2.googleapis.com/token`)

### 3. XML Configuration

Store configuration XML in Supabase storage with structure:
```xml
<config>
  <supabase>
    <tableName>your_table_name</tableName>
  </supabase>
  <mail>
    <sender>sender@example.com</sender>
    <to>recipient@example.com</to>
    <subject>Processing Complete</subject>
    <message_text>Data processing finished successfully.</message_text>
  </mail>
</config>
```

## Usage

### Manual Execution
```bash
python github_actions_pipeline.py
```

### GitHub Actions
- **Automatic**: Runs daily at 2 AM UTC
- **Manual**: Push to main/master branch or create PR
- **Scheduled**: Configurable via cron expression in workflow

## Workflow

1. **Authentication**: Connects to Gmail and Supabase
2. **Email Search**: Looks for emails with subject "Dealer Transactions Report" and CSV attachments
3. **Data Processing**: 
   - Downloads CSV attachments
   - Cleans financial columns (removes $, commas)
   - Standardizes datetime formats
   - Handles missing values
4. **Database Upload**: 
   - Uploads in 1000-record chunks
   - Implements retry logic for failed chunks
   - Tracks success/failure rates
5. **Logging**: Records processing history in TransactionLog table
6. **Cleanup**: Removes temporary files from storage
7. **Notifications**: Sends email updates on start/completion

## Data Processing

### Financial Columns Cleaned
- Amount, TotalAmount, Surcharge, MSF, Tip
- Cashout, Extras, Levy, ServiceFee, TxnFee, Rebate

### DateTime Handling
- Supports multiple formats: `dd/mm/yyyy HH:MM`, `dd/mm/yy HH:MM`
- Converts to standardized YYYY-MM-DD format
- Handles parsing errors gracefully

### Error Handling
- **Timeout Management**: 55-minute limit for GitHub Actions
- **Retry Logic**: Failed chunks retried up to 2 times
- **Duplicate Prevention**: Checks TransactionLog to avoid reprocessing
- **Comprehensive Logging**: All operations logged with status indicators

## Monitoring

### Log Output
```
=� Starting GitHub Actions Data Pipeline
=� Loading XML configuration...
 Configuration loaded - Target table: transactions
=� Processing email attachments...
=� Found 2 matching emails
=� Loaded 1500 rows from dealer_report_20241220.csv
=� Uploading 1500 records to transactions
 Pipeline completed successfully
```

### Artifacts
- Processing logs
- CSV files (if generated)
- JSON reports

## Project Structure

```
.
   github_actions_pipeline.py  # Main pipeline script
   requirements.txt            # Python dependencies  
   pyproject.toml             # Project configuration
   README.md                  # This file
   .github/
       workflows/
           data-pipeline.yml  # GitHub Actions workflow
```

## Troubleshooting

### Common Issues

1. **Missing Dependencies**: Run `pip install -r requirements.txt`
2. **Authentication Errors**: Verify all GitHub secrets are set correctly
3. **Timeout Issues**: Pipeline automatically handles 60-minute GitHub Actions limit
4. **Database Errors**: Check Supabase connection and table permissions

### Local Testing

```bash
# Set environment variables
export SUPABASE_URL="your_url"
export SUPABASE_KEY="your_key"
# ... other variables

# Run pipeline
python github_actions_pipeline.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test locally
4. Submit a pull request

## License

MIT License - see LICENSE file for details