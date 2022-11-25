import pytz
from twilio.rest import Client


class TextNotification(object):
    def __init__(self):

        self.client = Client()
        self.from_number = "+14158497908"
        self.tz = pytz.timezone("US/Eastern")

    def send_notification(self, to: str, message: str):
        self.client.messages.create(from_=self.from_number, to=to, body=message)
