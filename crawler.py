import os
import requests
import shutil
from multiprocessing import Pool
import argparse
from collect_links import CollectLinks
import imghdr
import base64


class SearchEngines:
    Google = 1
    Google_HDR = 2
    Bing = 3
    Bing_HDR = 4

    @staticmethod
    def get_input(code):
        if code == SearchEngines.Google:
            return 'google'
        elif code == SearchEngines.Google_HDR:
            return 'google'
        elif code == SearchEngines.Bing:
            return 'bing'
        elif code == SearchEngines.Bing_HDR:
            return 'bing'


class Crawler:
    def __init__(self, pass_already_exist=True, no_threads=4, download_google=True, download_bing=True,
                 download_folder='Download', high_resolution=True):
        self.leave = pass_already_exist
        self.no_threads = no_threads
        self.download_google = download_google
        self.download_bing = download_bing
        self.download_folder = download_folder
        self.high_resolution = high_resolution

        os.makedirs('./{}'.format(self.download_folder), exist_ok=True)

    @staticmethod
    def all_directories(path):
        paths = []
        for roots, dirs, files in os.walk(path):
            for file in files:
                if os.path.isfile(path + '/' + file):
                    paths.append(path + '/' + file)

        return paths

    @staticmethod
    def get_extension_from_url(url, default='jpg'):
        url_split = str(url).split('.')
        if len(url_split) == 0:
            return default
        extension = url_split[-1].lower()
        if extension == 'jpg' or extension == 'jpeg':
            return 'jpg'
        elif extension == 'gif':
            return 'gif'
        elif extension == 'png':
            return 'png'
        else:
            return default

    @staticmethod
    def image_validation(path):
        extension = imghdr.what(path)
        if extension == 'jpeg':
            extension = 'jpg'
        return extension  # returns none if invalid

    @staticmethod
    def create_directory(directory_name):
        current_directory = os.getcwd()
        directory = os.path.join(current_directory, directory_name)
        if not os.path.exists(directory):
            os.makedirs(directory)

    @staticmethod
    def get_keywords(keywords_file='search_keywords.txt'):  # read search keywords from a text file
        with open(keywords_file, 'r', encoding='utf-8-sig') as key:
            text = key.read()
            new_lines = text.split('\n')
            new_lines = filter(lambda z: z != '' and z is not None, new_lines)
            keywords = sorted(set(new_lines))

        print('{} keywords obtained: {}'.format(len(keywords), keywords))

        # save sorted keywords again
        with open(keywords_file, 'w+', encoding='utf-8') as key:
            for keyword in keywords:
                key.write('{}\n'.format(keyword))

        return keywords

    @staticmethod
    def save_items_to_file(object, file_path, is_base64=False):
        try:
            with open('{}'.format(file_path), 'wb') as file:
                if is_base64:
                    file.write(object)
                else:
                    shutil.copyfileobj(object.raw, file)
        except Exception as excep:
            print('Save failed - {}'.format(excep))

    @staticmethod
    def base64_to_item(src):
        header, encoded = str(src).split(',', 1)
        data = base64.decodebytes(bytes(encoded, encoding='utf-8'))
        return data

    def download_images(self, keyword, urls, site_name):
        self.create_directory('{}/{}'.format(self.download_folder, keyword))
        total = len(urls)

        for index, link in enumerate(urls):
            try:
                print('Downloading {} from {}: {} / {}'.format(keyword, site_name, index + 1, total))

                if str(link).startswith('data:image/jpeg;base64'):
                    response = self.base64_to_item(link)
                    ext = 'jpg'
                    is_base64 = True
                elif str(link).startswith('data:image/png;base64'):
                    response = self.base64_to_item(link)
                    ext = 'png'
                    is_base64 = True
                else:
                    response = requests.get(link, stream=True)
                    ext = self.get_extension_from_url(link)
                    is_base64 = False

                no_ext_path = '{}/{}/{}_{}'.format(self.download_folder, keyword, site_name, str(index).zfill(4))
                path = no_ext_path + '.' + ext
                self.save_object_to_file(response, path, is_base64 =is_base64)

                del response

                ext_2 = self.image_validation(path)
                if ext_2 is None:
                    print('Unreadable file - {}'.format(link))
                    os.remove(path)
                else:
                    if ext != ext_2:
                        path_2 = no_ext_path + '.' + ext_2
                        os.rename(path, path_2)
                        print('Extension renamed {} -> {}'.format(ext, ext_2))

            except Exception as excep:
                print('Download Failed - ', excep)
                continue

    def downlaod_from_site(self, keyword, site_code):
        site_name = SearchEngines.get_text(site_code)
        add_url = SearchEngines.get_face_url(site_code)

        try:
            collect = CollectLinks()  # initialize Chrome driver
        except Exception as excep:
            print('Unable to initialize Chrome driver - {}'.format(excep))
            return

        try:
            print('Collecting links...{} from {}'.format(keyword, site_name))

            if site_code == SearchEngines.Google:
                links = collect.google(keyword, add_url)

            elif site_code == SearchEngines.Bing:
                links = collect.bing(keyword, add_url)

            elif site_code == SearchEngines.Google_HDR:
                links = collect.google_full(keyword, add_url)

            elif site_code == SearchEngines.Bing_HDR:
                links = collect.bing_full(keyword, add_url)

            else:
                print('Invalid Site Code')
                links = []

            print('Downloading images from Collected links...{} from {}'.format(keyword, site_name))
            self.download_images(keyword, links, site_name)

            print('Done {} : {}'.format(site_name, keyword))

        except Exception as excep:
            print('Exception {}:{} - {}'.format(site_name, keyword, excep))

    def download(self, arg):
        self.downlaod_from_site(keyword=arg[0], site_code=arg[1])

    def perform_crawling(self):
        keywords = self.get_keywords()

        tasks = []

        for keyword in keywords:
            directory_name = '{}/{}'.format(self.download_folder, keyword)
            if os.path.exists(os.path.join(os.getcwd(), directory_name)) and self.leave:
                print('Skipping already existing directory {}'.format(directory_name))
                continue

            if self.download_google:
                if self.high_resolution:
                    tasks.append([keyword, SearchEngines.Google_HDR])
                else:
                    tasks.append([keyword, SearchEngines.Google])

            if self.download_bing:
                if self.high_resolution:
                    tasks.append([keyword, SearchEngines.Bing_HDR])
                else:
                    tasks.append([keyword, SearchEngines.Bing])

        pool = Pool(self.no_threads)
        pool.map_async(self.download, tasks)
        pool.close()
        pool.join()
        print('Task completed. Pool Join.')

        self.imbalance_check()

        print('End Program')

    def imbalance_check(self):
        print('Data imbalance checking...')

        dict_num_files = {}

        for directory in self.all_directories(self.download_folder):
            n_files = len(self.all_files(directory))
            dict_num_files[directory] = n_files

        avg = 0
        for directory, n_files in dict_num_files.items():
            avg += n_files / len(dict_num_files)
            print('directory: {}, file_count: {}'.format(directory, n_files))

        dict_too_small = {}

        for directory, n_files in dict_num_files.items():
            if n_files < avg * 0.5:
                dict_too_small[directory] = n_files

        if len(dict_too_small) >= 1:
            print('Data imbalance found.')
            print('Following keywords have file count smaller than 50% of average.')
            print('I suggest you to remove these directories and try again for that keyword.')
            print('_________________________________')
            print('Too small file count directories:')
            for directory, n_files in dict_too_small.items():
                print('directory: {}, file_count: {}'.format(directory, n_files))

            print("Remove directories above? (y/n)")
            answer = input()

            if answer == 'y':
                # removing directories of small files
                print("Removing directories with too small file counts...")
                for directory, n_files in dict_too_small.items():
                    shutil.rmtree(directory)
                    print('Removed {}'.format(directory))

                print('Re-start this program to re-download the removed files.')
            else:
                print('Data imbalance not found')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip', type=str, default='true',
                        help='Skips keywords already downloaded. Consider when re-downloading.')
    parser.add_argument('--threads', type=int, default=4, help='Number of threads to download.')
    parser.add_argument('--google', type=str, default='true', help='Download from google.com (boolean)')
    parser.add_argument('--bing', type=str, default='true', help='Download from bing.com (boolean)')
    parser.add_argument('--full', type=str, default='false',
                        help='Download high resolution image instead of thumbnails (slow)')
    args = parser.parse_args()

    _skip = False if str(args.skip).lower() == 'false' else True
    _threads = args.threads
    _google = False if str(args.google).lower() == 'false' else True
    _bing = False if str(args.bing).lower() == 'false' else True
    _full = False if str(args.full).lower() == 'false' else True

    print(
        'Options - skip:{}, threads:{}, google:{}, bing:{}, high_resolution:{}'.format(_skip, _threads, _google, _bing,
                                                                                       _full))

    auto_crawler = Crawler(pass_already_exist=_skip, no_threads=_threads, download_google=_google, download_bing=_bing,
                           high_resolution=_full)
    auto_crawler.perform_crawling()
