from abc import ABC, abstractmethod


class BaseTemplate(ABC):

    @property
    def rendered(self):
        raise NotImplementedError

    @rendered.setter
    @abstractmethod
    def rendered(self, val):
        raise NotImplementedError

    @rendered.getter
    @abstractmethod
    def rendered(self):
        raise NotImplementedError

    def __repr__(self):
        return str(self.rendered)

    def to_dict(self):
        return self.rendered


class PlainTextSection(BaseTemplate):

    def __init__(self, text):
        self.template = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ""
            }
        }
        self.text = text

    @property
    def rendered(self):
        r = self.template
        r["text"]['text'] = self.text
        return r


class BaseField(BaseTemplate):

    def __init__(self, type, text):
        self.template = {
        "type": "",
        "text": ""
        }
        self.type = type
        self.text = text

    @property
    def rendered(self):
        r = self.template
        r['type'] = self.type
        r['text'] = self.text
        return r


class StandardField(BaseField):
    def __init__(self, title, text):
        formatted_text = "*{}*\n{}".format(title, text)
        super().__init__(type="mrkdwn",
                         text=formatted_text)


class FieldCollectionSection(BaseTemplate):

    def __init__(self, fields=[]):
        self.template = {"type": "section",
                         "fields": []
        }
        self.fields = fields

    def append(self, field):
        self.fields.append(field)

    @property
    def rendered(self):
        r = self.template
        r['fields'] = [f.rendered for f in self.fields]
        return r


class ButtonSection:

    def __init__(self, strapline, label, url=None):
        self.strapline = strapline
        self.label = label
        self.url = url
        self.template = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "{{button_strapline}}"
        },
        "accessory": {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "{{button_label}}"
            }
        }}

    @property
    def rendered(self):
        r = self.template
        r['text']['text'] = self.strapline
        r['accessory']['text']['text'] = self.label
        if self.url:
            r['accessory']['url'] = self.url

        return r


class BlockCollection(BaseTemplate):

    def __init__(self):
        self.blocks = []

    def append(self, val):
        self.blocks.append(val)

    @property
    def rendered(self):
        return [t.rendered for t in self.blocks]


class SlackWebHookMessage(BaseTemplate):

    def __init__(self, channel, blocks):
        self.template = {"channel": "",
                         "blocks": []}
        self.channel = channel
        self.blocks = blocks

    @property
    def rendered(self):
        r = self.template
        r['channel'] = self.channel
        r['blocks'] = self.blocks.rendered
        return r

