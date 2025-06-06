from pydantic import BaseModel, Field
import time
class ProduceMessage(BaseModel):
    post_content_text: str =Field(min_length=1,max_length=250)
    social_media_platform: str  =Field(min_length=1,max_length=250)
    producer_id: str  =Field(min_length=1,max_length=250)
    language: str = Field(min_length=1, max_length=50)
    no_of_hashtags: int = Field(default=0)
    no_of_mentions: int = Field(default=0)
    

