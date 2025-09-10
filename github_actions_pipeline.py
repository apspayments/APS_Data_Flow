#!/usr/bin/env python3
"""
GitHub Actions Optimized Financial Data Pipeline
Main pipeline code - save as: github_actions_pipeline.py

This script:
1. Authenticates with Gmail and Supabase
2. Searches for transaction emails with CSV attachments
3. Downloads and processes CSV files
4. Cleans financial data and standardizes datetime formats
5. Uploads to Supabase database in chunks with retry logic
6. Sends email notifications and logs processing history
7. Handles GitHub Actions timeout constraints
"""

import base64
import io
import json
import logging
import os
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime
from email.message import EmailMessage
from typing import Dict, List, Optional
import time

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from supabase import create_client, Client

# Simple logging setup for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

processed_files = []
codes = ["Q47445", "Q47447", "Q47448"]

SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
]

class TimeoutHandler:
    """Handle GitHub Actions 60-minute timeout"""
    
    def __init__(self, timeout_minutes: int = 55):
        self.timeout_seconds = timeout_minutes * 60
        self.start_time = time.time()
    
    def check_timeout(self) -> bool:
        """Check if approaching timeout"""
        elapsed = time.time() - self.start_time
        remaining = self.timeout_seconds - elapsed
        
        if remaining < 300:  # 5 minutes warning
            logger.warning(f"Approaching timeout: {remaining/60:.1f} minutes remaining")
            return True
        return False

class GitHubActionsDataPipeline:
    """GitHub Actions optimized data pipeline"""
    
    def __init__(self):
        self.timeout_handler = TimeoutHandler()
        self.supabase = None
        self.gmail = None
        
        # Load configuration from environment
        self.config = self._load_config()
        
        # Statistics
        self.files_processed = 0
        self.rows_processed = 0
        self.rows_uploaded = 0
        self.errors_count = 0
    
    def _load_config(self) -> Dict:
        """Load configuration from environment variables"""
        required_vars = [
            'SUPABASE_URL', 'SUPABASE_KEY', 'BUCKET_NAME', 'FILE_PATH',
            'REFRESH_TOKEN', 'CLIENT_ID', 'CLIENT_SECRET', 'TOKEN_URI'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
           logger.error(f"Missing required environment variables: {missing_vars}")
           sys.exit(1)
        
        return {
            'supabase_url': os.getenv('SUPABASE_URL'),
            'supabase_key': os.getenv('SUPABASE_KEY'),
            'bucket_name': os.getenv('BUCKET_NAME'),
            'file_path': os.getenv('FILE_PATH'),
            'refresh_token': os.getenv('REFRESH_TOKEN'),
            'client_id': os.getenv('CLIENT_ID'),
            'client_secret': os.getenv('CLIENT_SECRET'),
            'token_uri': os.getenv('TOKEN_URI'),
            'table_name': '',
            'sender': '',
            'recipient': '',
            'subject': '',
            'message_text': '' 
        }
    
    def _init_supabase(self):
        """Initialize Supabase client"""
        if self.supabase is None:
            try:
                self.supabase = create_client(self.config['supabase_url'], self.config['supabase_key'])
                logger.info("✅ Supabase connection established")
            except Exception as e:
                logger.error(f"❌ Supabase initialization failed: {e}")
                raise
    
    def _init_gmail(self):
        """Initialize Gmail service"""
        if self.gmail is None:
            try:     

                print('refresh_token:' +self.config['refresh_token'])
                print('token_uri:' +self.config['token_uri'])
                print('client_id:' +self.config['client_id'])
                print('client_secret:' +self.config['client_secret'])

                creds = Credentials(
                    token=None,
                    refresh_token=self.config['refresh_token'],
                    token_uri=self.config['token_uri'],
                    client_id=self.config['client_id'],
                    client_secret=self.config['client_secret'],
                    scopes=SCOPES
                   
                )                
                print(creds)
                creds.refresh(Request())
                self.gmail = build('gmail', 'v1', credentials=creds)
                logger.info("✅ Gmail service authenticated")
            except Exception as e:
                logger.error(f"❌ Gmail authentication failed: {e}")
                raise
    
    def load_xml_config(self) -> bool:
        """Load XML configuration from Supabase storage"""
        try:
            logger.info("📋 Loading XML configuration...")
            self._init_supabase()
            
            storage = self.supabase.storage.from_(self.config['bucket_name'])
            file_bytes = storage.download(self.config['file_path'])
            xml_str = file_bytes.decode('utf-8')
            root = ET.fromstring(xml_str)
            
            # Update config with XML values
            self.config['table_name'] = root.find('./supabase/tableName').text
            self.config['sender'] = root.find('./mail/sender').text
            self.config['recipient'] = root.find('./mail/to').text
            self.config['subject'] = root.find('./mail/subject').text
            self.config['message_text'] = root.find('./mail/message_text').text
            
            logger.info(f"✅ Configuration loaded - Target table: {self.config['table_name']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ XML config loading failed: {e}")
            self.errors_count += 1
            return False
    
    def process_emails(self) -> bool:
        """Process Gmail attachments"""
        try:
            logger.info("📧 Processing email attachments...")
            self._init_gmail()
            
            today = datetime.today().strftime('%Y/%m/%d')
            query = f'after:{today} filename:csv has:attachment subject:"Dealer Transactions Report"'
            
            logger.info(f"🔍 Searching emails with query: {query}")
            
            results = self.gmail.users().messages().list(userId='me', q=query).execute()
            messages = results.get('messages', [])
            
            logger.info(f"📬 Found {len(messages)} matching emails")
            
            if not messages:
                logger.info("📭 No matching emails found")
                return True
            
            # Cleanup existing files first
            self._cleanup_storage_folder("Data")
            
            # Process each message
            for i, message in enumerate(messages):
                if self.timeout_handler.check_timeout():
                    logger.warning(f"⏰ Timeout approaching - processed {i}/{len(messages)} emails")
                    break
                
                logger.info(f"📨 Processing email {i+1}/{len(messages)}")
                self._process_single_message(message, today)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Email processing failed: {e}")
            self.errors_count += 1
            return False
    
    def _process_single_message(self, message: Dict, today: str):
        """Process a single email message"""
        try:
            msg_details = self.gmail.users().messages().get(userId='me', id=message['id']).execute()
            
            # Extract plain text from email body
            plain_text = self._extract_message_text(msg_details)
            
            # Process attachments
            for part in msg_details['payload'].get('parts', []):
                filename = part.get("filename")
                body = part.get("body", {})
                
                if not filename or not filename.endswith('.csv') or 'attachmentId' not in body:
                    continue
                
                # Check for duplicates
                if self._check_duplicate_processing(today, filename):
                    logger.info(f"⏭️  File {filename} already processed for {today}")
                    continue
                
                logger.info(f"📎 Processing attachment: {filename}")
                self._process_attachment(message['id'], body['attachmentId'], filename, today, plain_text)
                
        except Exception as e:
            logger.error(f"❌ Message processing error: {e}")
            self.errors_count += 1
    
    def _extract_message_text(self, msg_details: Dict) -> str:
        """Extract plain text from email message"""
        try:
            for part in msg_details['payload'].get('parts', []):
                if part.get("mimeType") == "text/html":
                    body_data = part.get("body", {}).get("data")
                    if body_data:
                        decoded_body = base64.urlsafe_b64decode(body_data).decode("utf-8")
                        soup = BeautifulSoup(decoded_body, "html.parser")
                        return soup.get_text(separator="\n").strip()
            return ""
        except Exception as e:
            logger.warning(f"⚠️  Could not extract email text: {e}")
            return ""
    
    def _check_duplicate_processing(self, date: str, filename: str) -> bool:
        """Check if file already processed today"""
        try:
            response = (self.supabase.table("TransactionLog")
                       .select("*")
                       .eq("filedate", date)
                       .eq("filename", filename)
                       .limit(1)
                       .execute())
            return bool(response.data)
        except Exception as e:
            logger.warning(f"⚠️  Duplicate check failed: {e}")
            return False
    
    def _process_attachment(self, message_id: str, attachment_id: str, filename: str, 
                          file_date: str, email_subject: str):
        """Process a single CSV attachment"""
        try:
            # Download attachment
            attachment = self.gmail.users().messages().attachments().get(
                userId='me', messageId=message_id, id=attachment_id
            ).execute()
            
            attachment_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
            
            # Upload to Supabase storage
            storage = self.supabase.storage.from_(self.config['bucket_name'])
            file_path = f"Data/{filename}"
            storage.upload(file_path, attachment_data, {"content-type": "text/csv"})
            
            # Load CSV into DataFrame
            csv_str = attachment_data.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_str))
            
            num_rows = len(df)
            self.files_processed += 1
            self.rows_processed += num_rows
            
            logger.info(f"📊 Loaded {num_rows} rows from {filename}")
            
            # Send start notification
            self._send_email_notification(
                subject=f"{self.config['subject']} started for {filename} with {num_rows} rows",
                body=self.config['message_text']
            )
            try:
                processed_files.append({"filename": filename, "rows": num_rows})
            except Exception as e:
                logger.warning(f"⚠️ Failed to append processed file record: {e}")
            # Process the data
            df_processed = self._process_dataframe(df)
            
            # Upload to database with hybrid retry strategy
            successful_rows = self._upload_to_database_with_retry(df_processed)
            self.rows_uploaded += successful_rows
            
            # Log the processing
            self._log_processing(filename, file_date, num_rows, successful_rows, email_subject)
            
            # Cleanup temporary file
            storage.remove([file_path])
            
            # Send completion notification
            self._send_email_notification(
                subject=f"{self.config['subject']} completed for {filename} with {successful_rows} rows",
                body=self.config['message_text']
            )
            
            logger.info(f"✅ Successfully processed {filename}: {successful_rows}/{num_rows} rows uploaded")
            
        except Exception as e:
            logger.error(f"❌ Failed to process attachment {filename}: {e}")
            self.errors_count += 1
    
    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and clean DataFrame (same as original script)"""
        logger.info("🧹 Processing and cleaning data...")
        
        df_filtered = df.copy()
        
        # Clean financial columns
        financial_columns = ['Amount', 'TotalAmount', 'Surcharge', 'MSF', 'Tip', 
                           'Cashout', 'Extras', 'Levy', 'ServiceFee', 'TxnFee', 'Rebate']
        
        existing_columns = [col for col in financial_columns if col in df_filtered.columns]
        
        if existing_columns:
            df_filtered[existing_columns] = (df_filtered[existing_columns]
                                           .replace(r'[\$,]', '', regex=True)
                                           .astype(float, errors='ignore'))
            
            # Convert negative MSF to positive
            if 'MSF' in df_filtered.columns:
                df_filtered['MSF'] = df_filtered['MSF'].apply(lambda x: abs(x) if x < 0 else x)
        
        # Process datetime column
        if 'TransactionDatetime' in df_filtered.columns:
            df_filtered['TransactionDatetime'] = df_filtered['TransactionDatetime'].apply(self._convert_datetime)
            df_filtered['TransactionDatetime'] = pd.to_datetime(df_filtered['TransactionDatetime'], errors='coerce').dt.date
            
            # Log conversion stats
            successful = df_filtered['TransactionDatetime'].notna().sum()
            failed = df_filtered['TransactionDatetime'].isna().sum()
            logger.info(f"📅 DateTime conversion: {successful} successful, {failed} failed")
        
        # Replace NaN with None for database
        df_filtered = df_filtered.replace({np.nan: None})
        
        return df_filtered
    
    def _convert_datetime(self, dt_str):
        """Convert datetime string to standardized format (same as original)"""
        if pd.isna(dt_str) or str(dt_str).lower() == 'nan':
            return pd.NaT

        dt_str = str(dt_str).strip()

        # Try common date formats
        formats_to_try = [
            '%d/%m/%Y %H:%M',  # 30/04/2025 19:36
            '%d/%m/%y %H:%M',  # 12/4/25 20:46
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d'
        ]

        for fmt in formats_to_try:
            try:
                dt = pd.to_datetime(dt_str, format=fmt)
                return dt.strftime('%Y-%m-%d')
            except:
                continue

        # Fallback to auto-detection
        try:
            return pd.to_datetime(dt_str, dayfirst=True)
        except:
            return pd.NaT
    
    def _upload_to_database_with_retry(self, df: pd.DataFrame) -> int:
        """Upload DataFrame to database with hybrid retry strategy"""
        logger.info(f"💾 Uploading {len(df)} records to {self.config['table_name']}")
        
        # Convert to records
        records = df.to_dict('records')
        
        # Handle date serialization
        for record in records:
            for key, value in record.items():
                if hasattr(value, 'isoformat'):
                    record[key] = value.isoformat()
        
        # Hybrid retry strategy: limited retries with timeout awareness
        chunk_size = 1000
        max_retries = 2
        total_rows = len(records)
        successful_rows = 0
        
        logger.info(f"📦 Using hybrid retry strategy: {chunk_size} records/chunk, {max_retries} max retries")
        
        # Upload in chunks with retry logic
        for i in range(0, total_rows, chunk_size):
            if self.timeout_handler.check_timeout():
                logger.warning("⏰ Timeout approaching - stopping upload")
                break
            
            chunk_num = (i // chunk_size) + 1
            total_chunks = (total_rows + chunk_size - 1) // chunk_size
            end_idx = min(i + chunk_size, total_rows)
            chunk = records[i:end_idx]
            
            # Try up to max_retries times for this chunk
            chunk_successful = False
            for attempt in range(max_retries):
                try:
                    response = self.supabase.table(self.config['table_name']).insert(chunk).execute()
                    if response.data:
                        successful_rows += len(response.data)
                        chunk_successful = True
                        progress = (i + len(chunk)) / total_rows * 100
                        logger.info(f"✅ Chunk {chunk_num}/{total_chunks}: {len(chunk)} records uploaded (attempt {attempt + 1}) - {progress:.1f}% complete")
                        break
                    else:
                        raise Exception("No data returned from insert")
                        
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"⚠️  Chunk {chunk_num} attempt {attempt + 1} failed: {e}")
                        logger.info(f"   Retrying immediately...")
                        time.sleep(1)  # Brief pause
                    else:
                        logger.error(f"❌ Chunk {chunk_num} failed after {max_retries} attempts: {e}")
                        logger.info(f"   Lost {len(chunk)} records, continuing with next chunk...")
            
            if not chunk_successful:
                logger.warning(f"   Chunk {chunk_num} permanently failed - {len(chunk)} records lost")
        
        success_rate = (successful_rows / total_rows) * 100 if total_rows > 0 else 0
        logger.info(f"📊 Upload complete: {successful_rows}/{total_rows} records ({success_rate:.1f}% success rate)")
        
        return successful_rows
    
    def _log_processing(self, filename: str, file_date: str, num_rows: int, 
                       successful_rows: int, email_subject: str):
        """Log processing details to TransactionLog table"""
        try:
            log_data = {
                "filename": filename,
                "filedate": file_date,
                "num_rows": num_rows,
                "successful_rows": successful_rows,
                "subject": email_subject,
                "processed_at": datetime.now().isoformat()
            }
            
            self.supabase.table("TransactionLog").insert(log_data).execute()
            logger.info("📋 Processing logged to TransactionLog")
            
        except Exception as e:
            logger.warning(f"⚠️  Logging failed: {e}")
    
    def _send_email_notification(self, subject: str, body: str):
        """Send email notification"""
        try:
            message = EmailMessage()
            message.set_content(body)
            message['To'] = self.config['recipient']
            message['From'] = self.config['sender']
            message['Subject'] = subject
            
            encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            self.gmail.users().messages().send(
                userId='me', body={'raw': encoded_message}
            ).execute()
            
            logger.info(f"📧 Email notification sent: {subject}")
            
        except Exception as e:
            logger.warning(f"⚠️  Email notification failed: {e}")
    
    def _send_email_notification1(self, subject: str, body: str):
        """Send email notification"""
        try:
            message = EmailMessage()
            message.set_content(body)
            message['To'] =  "gio@aps.business, alana@aps.business, damien.meyepa@livepayments.com, aps@aps.business"
            message['From'] = self.config['sender']
            message['Subject'] = subject
            
            encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            self.gmail.users().messages().send(
                userId='me', body={'raw': encoded_message}
            ).execute()
            
            logger.info(f"📧 Email notification sent: {subject}")
            
        except Exception as e:
            logger.warning(f"⚠️  Email notification failed: {e}")

    def _cleanup_storage_folder(self, folder_path: str):
        """Remove all files from storage folder"""
        try:
            storage = self.supabase.storage.from_(self.config['bucket_name'])
            files_response = storage.list(path=folder_path)
            
            if hasattr(files_response, 'error') and files_response.error:
                return
            
            files = files_response.data if hasattr(files_response, 'data') else files_response
            file_paths = [f"{folder_path}/{file['name']}" for file in files 
                         if not file['name'].startswith(".")]
            
            if file_paths:
                # Delete in batches
                batch_size = 20
                for i in range(0, len(file_paths), batch_size):
                    batch = file_paths[i:i + batch_size]
                    storage.remove(batch)
                
                logger.info(f"🗑️  Cleaned up {len(file_paths)} files from {folder_path}")
                
        except Exception as e:
            logger.warning(f"⚠️  Cleanup warning: {e}")
    
    def run(self) -> bool:
        """Run the complete pipeline"""
        start_time = datetime.now()
        
        try:
            logger.info("🚀 Starting GitHub Actions Data Pipeline")
            
            # Load XML configuration
            if not self.load_xml_config():
                return False
            
            # Process emails and attachments
            if not self.process_emails():
                return False
            
            # Log final results
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("📊 Pipeline Summary:")
            logger.info(f"   Duration: {duration:.1f}s")
            logger.info(f"   Files processed: {self.files_processed}")
            logger.info(f"   Rows processed: {self.rows_processed}")
            logger.info(f"   Rows uploaded: {self.rows_uploaded}")
            logger.info(f"   Errors: {self.errors_count}")
            
            if self.errors_count == 0:
                logger.info("✅ Pipeline completed successfully")
                return True
            else:
                logger.warning(f"⚠️  Pipeline completed with {self.errors_count} errors")
                return self.errors_count < self.files_processed  # Partial success acceptable
                
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return False

def check_codes(codes, processed_files):
    """
    Returns a single list of codes that are either:
    - found with rows == 0
    - missing from processed_files
    """
    found_codes = set()
    bad_codes = []

    for file in processed_files:
        code = file["filename"].split("_")[0]
        rows = file["rows"]

        found_codes.add(code)

        if rows == 0 and code in codes:
            bad_codes.append(code)

    # Add missing codes
    for code in codes:
        if code not in found_codes:
            bad_codes.append(code)

    return bad_codes

def main():
    """Main entry point"""
    try:
        pipeline = GitHubActionsDataPipeline()
        success = pipeline.run()
        
        # Exit with appropriate code for GitHub Actions
        if success:    
            try:
                result = check_codes(codes, processed_files)
                if result:
                    subject = "⚠️ Data Processing Alert"
                    body = "The below Dealer id was not processed today:\n\n" + "\n".join(result)
                    try:
                        # Call the method on the pipeline instance safely
                        pipeline._send_email_notification1(subject, body)
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to send alert email: {e}")
            except Exception as e:
                logger.warning(f"⚠️ Error while checking codes: {e}")

            logger.info("🎉 Pipeline execution successful")
            sys.exit(0)
        else:
            logger.error("💥 Pipeline execution failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 Application startup failed: {e}")
        sys.exit(2) 

if __name__ == "__main__":
    main()
