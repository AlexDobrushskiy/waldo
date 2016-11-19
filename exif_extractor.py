import requests
import xml.etree.ElementTree
import exifread
import sqlite3

IMAGE_ROOT_URL = 'http://s3.amazonaws.com/waldo-recruiting/'


class AmazonXMLParser(object):
    def __init__(self, url):
        self.url = url

    def get_image_names(self):
        response = requests.get(self.url)
        root = xml.etree.ElementTree.fromstring(response.content)
        img_names = []
        for el in root:
            if 'Contents' in el.tag:
                for contents_child in el:
                    if 'Key' in contents_child.tag:
                        img_names.append(contents_child.text)
                        break
        return img_names


class ExifTagProcessor(object):
    def __init__(self, image_name, image_binary_content):
        f = open(image_name, 'wb')
        f.write(image_binary_content)
        f.close()
        self.filename = image_name

    def get_tags(self):
        f = open(self.filename, 'r')
        tags = exifread.process_file(f)
        f.close()
        return tags


class DBExifSaver(object):
    """
    Creates and uses the following SQLite table:

    |----------------|---------------|-------------------|
    |photo_name text | exif_key text | exif_value text   |
    |----------------|---------------|-------------------|

    """
    DB_NAME = 'images_exif.db'

    def __enter__(self):
        self.conn = sqlite3.connect(self.DB_NAME)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE exif_data
                     (photo_name text, exif_key text, exif_value text)''')
        self.conn.commit()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def save_tag(self, img_name, exif_key, value):
        self.cursor.execute('''
            INSERT INTO exif_data VALUES ('{}', '{}', '{}')
            '''.format(img_name, exif_key, value))
        self.conn.commit()

if __name__ == '__main__':
    amazon_xml_parser = AmazonXMLParser(IMAGE_ROOT_URL)

    with DBExifSaver() as db_exif_saver:
        for img_name in amazon_xml_parser.get_image_names():
            response = requests.get(IMAGE_ROOT_URL+img_name)
            tag_processor = ExifTagProcessor(img_name, response.content)

            tags = tag_processor.get_tags()
            for key in tags.keys():
                if key not in ('JPEGThumbnail', 'TIFFThumbnail', 'Filename', 'EXIF MakerNote'):
                    db_exif_saver.save_tag(img_name, key, tags[key])
