from airflow._stub import EMAILS


def send_email(to, subject, html_content, files=None, **kwargs):
    EMAILS.append({"to": to, "subject": subject, "body": html_content, "files": files})
