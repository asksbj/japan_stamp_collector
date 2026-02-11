from models.base import BaseModel


class JPostOffice(BaseModel):

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get('id')
        self.name = kwargs.get('office_name')
        self.abolition = kwargs.get('abolition')
        self.prefecture = kwargs.get('prefecture')
        self.city = kwargs.get('city')
        self.address = kwargs.get('office_location')
        self.latitude = kwargs.get('latitude')
        self.longtitude = kwargs.get('longtitude')
        self.business_hour = kwargs.get('business_hour')


class Fuke(BaseModel):

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get('id')
        self.fuke_name = kwargs.get('fuke_name')
        self.abolition = kwargs.get('abolition')
        self.image_url = kwargs.get('image_url')
        self.detailed_url = kwargs.get('detailed_url')
        self.start_date = kwargs.get('start_date')
        self.description = kwargs.get('description')
        self.author = kwargs.get('author')

        self.post_office_id = kwargs.get('author')