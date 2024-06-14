import unittest
import sftp.sftp as sftpmodule
from datetime import date, timedelta
import dotenv
import os
import shutil

dotenv.load_dotenv()
host = os.getenv('SFTP_HOST')
user = os.getenv('SFTP_USER')
password = os.getenv('SFTP_PASSWORD')
sftp = sftpmodule.SFTP(host, user, password)


class Test_sftp(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists('./test_data/test_data_downloaded.xlsx'):
            os.remove('./test_data/test_data_downloaded.xlsx')

        if os.path.exists('./test_data/storage_tasks_copy'):
            shutil.rmtree('./test_data/storage_tasks_copy')

        try:
            sftp.rmtree(f'storage_tasks_sftp_test')
        except FileNotFoundError:
            pass
        sftp.mkdir(f'./storage_tasks_sftp_test')
        for f in [f'task_{date.today().isoformat()}_UBS_LARGE_NEW',
                  f'task_{(date.today()-timedelta(days=30)).isoformat()}_UBS_LARGE_NEW']:
            sftp.mkdir(f'./storage_tasks_sftp_test/{f}')

        sftp.put('./test_data/test_data.xlsx',
                 f'./storage_tasks_sftp_test/task_{date.today().isoformat()}'
                 f'_UBS_LARGE_NEW/test_data.xlsx')

    @classmethod
    def tearDownClass(cls):
        sftp.close()

    def test_listdir(self):
        files = [f for f in sftp.listdir('./storage_tasks_sftp_test') if f != 'task_2020-01-01_UBS_LARGE_NEW']
        self.assertEqual(len(files), 2)

    def test_mkdir(self):
        sftp.mkdir('./storage_tasks_sftp_test/test')
        files = [f for f in sftp.listdir('./storage_tasks_sftp_test') if f != 'task_2020-01-01_UBS_LARGE_NEW']
        self.assertEqual(len(files), 3)

        sftp.put('./test_data/test.txt', './storage_tasks_sftp_test/test/test.txt')
        files = sftp.listdir('./storage_tasks_sftp_test/test')
        self.assertEqual(len(files), 1)

    def test_get(self):
        sftp.get(f'./storage_tasks_sftp_test/task_{date.today().isoformat()}'
                 f'_UBS_LARGE_NEW/test_data.xlsx',
                 './test_data/test_data_downloaded.xlsx')
        self.assertTrue(os.path.exists('./test_data/test_data_downloaded.xlsx'))

    def test_copy_to_local(self):
        sftp.copy_to_local(f'./storage_tasks_sftp_test',
                           './test_data/storage_tasks_copy')
        self.assertTrue(os.path.exists('./test_data/storage_tasks_copy'))
        self.assertTrue(os.path.exists(f'./test_data/storage_tasks_copy/task_{date.today().isoformat()}'
                                       f'_UBS_LARGE_NEW'))
        self.assertTrue(os.path.exists(f'./test_data/storage_tasks_copy/task_{date.today().isoformat()}'
                                       f'_UBS_LARGE_NEW/test_data.xlsx'))

    def test_copy_to_remote(self):
        sftp.copy_to_remote('./test_data/task_2020-01-01_UBS_LARGE_NEW',
                            f'./storage_tasks_sftp_test/task_2020-01-01_UBS_LARGE_NEW')
        files = sftp.listdir('./storage_tasks_sftp_test/task_2020-01-01_UBS_LARGE_NEW')
        self.assertEqual(len(files), 1)

    def test_rename(self):
        sftp.rename('./storage_tasks_sftp_test/task_2020-01-01_UBS_LARGE_NEW',
                    './storage_tasks_sftp_test/task_2020-01-01_UBS_SMALL_NEW')
        files = sftp.listdir('./storage_tasks_sftp_test')
        self.assertTrue('task_2020-01-01_UBS_SMALL_NEW' in files)
        self.assertFalse('task_2020-01-01_UBS_LARGE_NEW' in files)
        sftp.rename('./storage_tasks_sftp_test/task_2020-01-01_UBS_SMALL_NEW',
                    './storage_tasks_sftp_test/task_2020-01-01_UBS_LARGE_NEW')