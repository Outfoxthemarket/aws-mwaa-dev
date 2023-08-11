from airflow.providers.smtp.hooks.smtp import SmtpHook


def dummy():
    with SmtpHook('SMTP_SENDGRID') as mail:
        mail.send_email_smtp(
            to=['rhea.bastian@homeenergy.co.uk', 'marcos.martinez@outfoxthemarket.co.uk'],
            subject='I am the subject',
            from_email='prettyspaniard@arriba.es',
            html_content='<p>Paella is not spicy</p>'
        )
