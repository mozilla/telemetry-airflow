from airflow.plugins_manager import AirflowPlugin
from airflow.utils.email import get_email_address_list
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate

import os
import logging

from airflow import configuration


# This is mostly a copy of the `send_email_smtp` method in airflow
# (https://github.com/apache/incubator-airflow/blob/f4f8027cbf61ce2ed6a9989facf6c99dffb12f66/airflow/utils/email.py#L58-L101)
# except the payload is encoded as `quoted-printable` and the end of the method logs the
# message instead of calling `send_MIME_email`
def log_email_backend(to, subject, html_content, files=None,
                      dryrun=False, cc=None, bcc=None,
                      mime_subtype='mixed', mime_charset='utf-8',
                      **kwargs):
    smtp_mail_from = configuration.conf.get('smtp', 'SMTP_MAIL_FROM')
    to = get_email_address_list(to)

    msg = MIMEMultipart(mime_subtype)
    msg['Subject'] = subject
    msg['From'] = smtp_mail_from
    msg['To'] = ", ".join(to)
    if cc:
        cc = get_email_address_list(cc)
        msg['CC'] = ", ".join(cc)

    if bcc:
        # don't add bcc in header
        bcc = get_email_address_list(bcc)

    msg['Date'] = formatdate(localtime=True)
    mime_text = MIMEText(None, 'plain', mime_charset)
    mime_text.replace_header('content-transfer-encoding', 'quoted-printable')
    mime_text.set_payload(html_content)
    msg.attach(mime_text)

    for fname in files or []:
        basename = os.path.basename(fname)
        with open(fname, "rb") as f:
            part = MIMEApplication(
                f.read(),
                Name=basename
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename
            part['Content-ID'] = '<%s>' % basename
            msg.attach(part)
    logger = logging.getLogger("plugin.log_email_backend")
    logger.info("\n" + msg.as_string())


class LogEmailBackendPlugin(AirflowPlugin):
    name = 'log_email_backend'
    # No, this isn't the best place for this but unfortunately there's only a limited
    # number of places we can inject an email backend into the airflow import path
    # and macros seemed like the best choice
    macros = [log_email_backend]
