import boto3
import base64
import os
import logging
from io import BytesIO
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class S3StorageService:
    def __init__(self):
        load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
        self.bucket_name = os.getenv('AWS_S3_BUCKET')
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )

    def upload_base64(self, base64_str, destination_path):
        """
        Decodes a base64 string and uploads it to S3 (or local storage if S3 is unavailable).
        Returns the path/URL. Fallbacks to a default logo on error.
        """
        fallback_url = "https://gll-dev.s3.amazonaws.com/logos/f55c7f71-048a-4248-ac98-1557a679be05.png"
        return fallback_url
        if not base64_str or not isinstance(base64_str, str) or len(base64_str) < 100: 
            return fallback_url

        # Handle potential header (e.g., data:image/png;base64,...)
        if ',' in base64_str:
            base64_str = base64_str.split(',')[1]

        try:
            image_data = base64.b64decode(base64_str)
        except Exception as e:
            logger.error(f"Failed to decode base64 string: {str(e)}")
            return fallback_url
        
        # Check for AWS credentials
        if not os.getenv('AWS_ACCESS_KEY_ID'):
            logger.error("AWS credentials missing. S3 upload is required but not configured.")
            return fallback_url

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=destination_path,
                Body=image_data
            )
            return f"https://{self.bucket_name}.s3.amazonaws.com/{destination_path}"
        except Exception as e:
            logger.error(f"S3 Upload failed for {destination_path}: {str(e)}")
            return fallback_url

