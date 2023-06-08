from time import sleep
from prefect_email import EmailServerCredentials
import os
def create_email_server_credentials():
    my_email_credentials = EmailServerCredentials(
        username=os.getenv('gmail_username'),
        password=os.getenv('gmail_pass'),
        smtp_server='gmail',
        smtp_type='SSL'
    )
    my_email_credentials.save(name='my-gmail-credentials')

if __name__ == '__main__':
    create_email_server_credentials()