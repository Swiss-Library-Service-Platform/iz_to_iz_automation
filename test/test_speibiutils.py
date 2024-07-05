import unittest
import sftp.sftp as sftpmodule
import dotenv
import os
import shutil

import speibiutils.speibiutils as speibi
import speibiutils.workflow as workflow
from datetime import date, timedelta

dotenv.load_dotenv()
host = os.getenv('SFTP_HOST')
user = os.getenv('SFTP_USER')
password = os.getenv('SFTP_PASSWORD')
sftp = sftpmodule.SFTP(host, user, password)
sftp.SFTP_Client.chdir(path='automation_storage_tasks')
log = speibi.LogFile()
SBK_DIR = speibi.SBK_DIR


class Test_speibiutils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Copy empty task summary
        if os.path.isfile('./data/task_summary.xlsx'):
            os.remove('./data/task_summary.xlsx')
        shutil.copy('./test_data/task_summary.xlsx', './data/task_summary.xlsx')

        for directory in SBK_DIR:

            # Clean local directories
            if os.path.exists(f'./data/{directory}'):
                shutil.rmtree(f'./data/{directory}')
            os.mkdir(f'./data/{directory}')
            os.mkdir(f'./data/{directory}/download')
            os.mkdir(f'./data/{directory}/download/storage_tasks')

            # Clean remote directories
            if sftp.is_dir(f'./{directory}'):
                sftp.rmtree(f'./{directory}')
            sftp.mkdir(f'./{directory}')
            sftp.mkdir(f'./{directory}/download')
            sftp.mkdir(f'./{directory}/upload')
            sftp.mkdir(f'./{directory}/download/storage_tasks')

    def test_get_remote_directories(self):

        remote_path = './sbkrzs/download/storage_tasks/task_2041-01-01_UBS_LARGE_NEW'
        sftp.mkdir(remote_path)
        directories = speibi.RemoteLocation().directories
        self.assertTrue('task_2041-01-01_UBS_LARGE_NEW' in directories, 'Remote directory should be available')
        self.assertTrue(sftp.is_dir(remote_path), 'Remote directory not created')

        # sftp.remove('./sbkrzs/download/storage_tasks/task_2041-01-01_UBS_LARGE_NEW')

    def test_tasks_summary_save(self):
        speibi.TaskSummary().save()
        self.assertTrue(sftp.is_file('./sbkubs/download/storage_tasks/task_summary.xlsx'),
                        'task_summary.xlsx not saved')

    def test_clean_local_directories(self):

        os.mkdir('./data/sbkzbs/download/storage_tasks/task_2000-01-01_ZBS_LARGE_NEW')
        os.mkdir(f'./data/sbkzbs/download/storage_tasks/task_{date.today().isoformat()}_ZBS_LARGE_NEW')

        speibi.TaskSummary().clean_local_directories()

        self.assertFalse(os.path.exists('./data/sbkzbs/download/storage_tasks/task_2000-01-01_ZBS_LARGE_NEW'),
                         'Old local directory should be deleted')
        self.assertTrue(
            os.path.exists(f'./data/sbkzbs/download/storage_tasks/task_{date.today().isoformat()}_ZBS_LARGE_NEW'),
            'New local directory should be kept')

        os.rmdir(f'./data/sbkzbs/download/storage_tasks/task_{date.today().isoformat()}_ZBS_LARGE_NEW')

    def test_get_task_parameters(self):
        sftp.mkdir('./sbkrzs/download/storage_tasks/task_2032-01-01_RZS_LARGE_NEW')
        remote = speibi.RemoteLocation()
        t = speibi.TaskSummary()
        remote_path = [p for p in remote.paths if 'task_2032-01-01_RZS_LARGE_NEW' in p][0]
        task = speibi.Task(remote_path)
        parameters = task.get_parameters()
        self.assertEqual(parameters['Account'], 'sbkrzs', 'Account not correct, must be sbkrzs')
        self.assertEqual(parameters['Scheduled_date'], '2032-01-01', 'Account not correct, must be sbkrzs')
        # sftp.remove('./sbkrzs/download/storage_tasks/task_2032-01-01_RZS_LARGE_NEW')

    def test_clean_remote_directories(self):
        sftp.mkdir('./sbkrzs/download/storage_tasks/task_2033-01-01_RZS_LARGE_NEW')
        sftp.mkdir('./sbkrzs/download/storage_tasks/task_2033-01-01_RZS_LARGE_NEW')
        sftp.mkdir('./sbkzbs/download/storage_tasks/task_2033-01-01_ZBS_LARGE_NEW')
        sftp.mkdir('./sbkrzs/download/storage_tasks/task_2034-01-01_RZS_LARGE_NEW')
        sftp.put('./test_data/test_data.xlsx',
                 './sbkrzs/download/storage_tasks/task_2034-01-01_RZS_LARGE_NEW/task_2034-01-01_RZS.xlsx')

        speibi.TaskSummary().clean_remote_directories()
        directories = speibi.RemoteLocation().directories
        # self.assertEqual(len(directories), 3, 'Remote directories not cleaned')
        self.assertTrue('task_2034-01-01_RZS_LARGE_NEW' in directories, 'Valid directory not available')
        self.assertTrue('task_2033-01-01_RZS_LARGE_NEW' in directories, 'Valid directory not available')
        self.assertTrue('task_2033-01-01_ZBS_LARGE_ERROR' in directories, 'Error not detected')

        # sftp.remove('./sbkrzs/download/storage_tasks/task_2034-01-01_RZS_LARGE_NEW')
        # sftp.remove('./sbkrzs/download/storage_tasks/task_2033-01-01_RZS_LARGE_NEW')
        # sftp.remove('./sbkzbs/download/storage_tasks/task_2033-01-01_ZBS_LARGE_ERROR')

    def test_check_forms_conformity(self):

        sftp.mkdir('./sbkuzh/download/storage_tasks/task_2050-01-01_UZH_LARGE_NEW')
        sftp.put('./test_data/test_data.xlsx',
                 './sbkuzh/download/storage_tasks/task_2050-01-01_UZH_LARGE_NEW/task_2050-01-01_UZH_LARGE.xlsx')

        task_summary = speibi.TaskSummary()
        task_summary.clean_remote_directories()
        task_summary.clean_local_directories()
        task_summary.check_forms_conformity()

        self.assertTrue(sftp.is_dir('./sbkuzh/download/storage_tasks/task_2050-01-01_UZH_LARGE_READY'),
                        'Valid task not created')
        self.assertTrue(sftp.is_file('./sbkuzh/download/storage_tasks/task_2050-01-01_UZH_LARGE_READY/'
                                     'form_check_result.txt'),
                        'Feedback txt file not created')
        self.assertTrue(os.path.exists('./data/sbkuzh/download/storage_tasks/task_2050-01-01_UZH_LARGE_READY/'
                                       'form_check_result.txt'),
                        'Local feedback txt file not created')

        sftp.rename('./sbkuzh/download/storage_tasks/task_2050-01-01_UZH_LARGE_READY',
                    './sbkuzh/download/storage_tasks/task_2050-01-01_UZH_LARGE_NEW')

        task_summary = speibi.TaskSummary()

        self.assertTrue('task_2050-01-01_UZH_LARGE_READY' in task_summary.get_directories(),
                        'Rename directory to new should be refleted in task summary')

        task_summary.clean_remote_directories()
        task_summary.clean_local_directories()
        task_summary = speibi.TaskSummary()

        self.assertTrue('task_2050-01-01_UZH_LARGE_NEW' in task_summary.get_directories(),
                        'Rename directory to new should bew refleted in task summary')

        task_summary.check_forms_conformity()

        self.assertFalse('task_2050-01-01_UZH_LARGE_NEW' in task_summary.get_directories(),
                         'State of task should be READY')

        self.assertTrue('task_2050-01-01_UZH_LARGE_READY' in task_summary.get_directories(),
                        'State of task should be READY')

    # def test_check_excel_file_conformity(self):
    #     result, barcodes, messages = speibi.check_form_file(
    #         'test_data/task_bad_today_UBS_LARGE_NEW/task_bad1_today_UBS.xlsx')
    #     self.assertFalse(result, 'Bad file should not be valid')
    #     self.assertTrue(
    #         "Bad or missing sheet names, must be ['General', 'Items', 'Locations_mapping', 'Item_policies_mapping', 'data_validation']" in messages,
    #         'Error message not found (tabs error)')
    #     self.assertTrue(len(barcodes) == 0, 'Barcodes should be empty')
    #
    #     result, barcodes, messages = speibi.check_form_file(
    #         'test_data/task_bad_today_UBS_LARGE_NEW/task_bad2_today_UBS.xlsx')
    #     self.assertFalse(result, 'Bad file should not be valid')
    #     self.assertTrue("Duplicate barcodes in the file: {'A1001180331'}" in messages,
    #                     'Error message not found (duplicate barcodes error)')
    #
    #     result, barcodes, messages = speibi.check_form_file(
    #         'test_data/task_today_UBS_LARGE_NEW/task_today_UBS.xlsx', ['A1001180331'])
    #     self.assertFalse(result, 'Bad file should not be valid')
    #     self.assertTrue("Barcodes {'A1001180331'} already in other tasks" in messages,
    #                     'Error message not found (barcode in other task)')
    #
    #     result, barcodes, messages = speibi.check_form_file(
    #         'test_data/task_today_UBS_LARGE_NEW/task_today_UBS.xlsx')
    #     self.assertTrue(result, 'Good file should be valid')
    #     self.assertTrue(len(barcodes) > 0, 'Barcodes should not be empty')
    #
    #     result, barcodes, messages = speibi.check_form_file(
    #         'test_data/task_today_UZB_SMALL_NEW/task_today_UZB.xlsx')
    #     self.assertTrue(result, 'Good file should be valid')
    #     self.assertEqual(len(barcodes), 2, 'Processing file should have 2 barcodes')

    def test_task_workflow_process(self):
        sftp.mkdir(f'./sbkhsg/download/storage_tasks/task_{date.today().isoformat()}_HSG_SMALL_NEW')
        sftp.put('./test_data/test_data.xlsx',
                 f'./sbkhsg/download/storage_tasks/task_{date.today().isoformat()}_HSG_SMALL_NEW/task_{date.today().isoformat()}_HSG_SMALL.xlsx')
        workflow.start('SMALL')

        self.assertTrue(sftp.is_dir(f'./sbkhsg/download/storage_tasks/task_{date.today().isoformat()}_HSG_SMALL_DONE'),
                        'Task should be done')

        task_summary = speibi.TaskSummary()
        self.assertTrue(task_summary.get_processing_task() is None, 'No task should be processing')
        self.assertFalse(task_summary.tasks.loc[
                             (task_summary.tasks['Directory'] == f'task_{date.today().isoformat()}_HSG_SMALL_DONE') &
                             (task_summary.tasks['State'] == 'DONE')].empty,
                         'Task should be done')

    def test_Task(self):
        t = speibi.Task(directory='task_2041-01-01_HSG_LARGE_NEW', account='sbkhsg')
        self.assertTrue(t.is_valid(), 'Provided task is valid')
        self.assertEqual(t.get_name(), 'task_2041-01-01_HSG_LARGE', 'Task name should be task_2041-01-01_HSG_LARGE')
        self.assertEqual(t.get_form_path(),
                         'sbkhsg/download/storage_tasks/task_2041-01-01_HSG_LARGE_NEW/task_2041-01-01_HSG_LARGE.xlsx',
                         'Form path should be sbkhsg/download/storage_tasks/task_2041-01-01_HSG_LARGE_NEW/task_2041-01-01_HSG_LARGE.xlsx')
        self.assertEqual(t.get_processing_file_path(local=True),
                         'data/sbkhsg/download/storage_tasks/task_2041-01-01_HSG_LARGE_NEW/task_2041-01-01_HSG_LARGE_items_processing.csv',
                         'Form path should be data/sbkhsg/download/storage_tasks/task_2041-01-01_HSG_LARGE_NEW/task_2041-01-01_HSG_LARGE_items_processing.csv')

        self.assertEqual(t.get_parameters()['Scheduled_date'], '2041-01-01', 'Scheduled date should be 2041-01-01')
        self.assertEqual(t.get_parameters()['State'], 'NEW', 'State should be NEW')
        self.assertEqual(t.get_parameters()['Account'], 'sbkhsg', 'Account should be sbkhsg')

    def test_check_excel_file_conformity(self):

        # Valid form
        os.mkdir('data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW')
        shutil.copy('./test_data/test_data.xlsx',
                    'data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE.xlsx')

        task = speibi.Task('sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW')

        result, barcodes, messages = task.check_form_file()
        self.assertTrue(result, 'Valid file should be valid')
        self.assertTrue(len(barcodes) > 0, 'Barcodes should not be empty')

        os.remove('data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE.xlsx')

        # Invalid form: bad tab
        shutil.copy('./test_data/test_data_bad1.xlsx',
                    'data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE.xlsx')

        result, barcodes, messages = task.check_form_file()

        self.assertTrue(
            "Bad or missing sheet names, must be ['General', 'Items', 'Locations_mapping', 'Item_policies_mapping', 'data_validation']" in messages,
            'Error message not found (tabs error)')
        self.assertTrue(len(barcodes) == 0, 'Barcodes should be empty')

        os.remove('data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE.xlsx')

        # Invalid form: duplicate barcode in same form
        shutil.copy('./test_data/test_data_bad2.xlsx',
                    'data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE.xlsx')

        result, barcodes, messages = task.check_form_file()

        self.assertFalse(result, 'Bad file should not be valid')
        self.assertTrue("Duplicate barcodes in the file: {'A1001180331'}" in messages,
                        'Error message not found (duplicate barcodes error)')

        os.remove('data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE.xlsx')

        # Invalid form: duplicate barcode checking multiple files
        shutil.copy('./test_data/test_data.xlsx',
                    'data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE.xlsx')

        result, barcodes, messages = task.check_form_file(['A1001180331'])

        self.assertFalse(result, 'Bad file should not be valid')
        self.assertTrue("Barcodes {'A1001180331'} already in other tasks" in messages,
                        'Error message not found (barcode in other task)')

        os.remove('data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE.xlsx')

        # Check with processing file

        shutil.copy('./test_data/task_today_UZB_SMALL_NEW/task_today_UZB_LARGE.xlsx',
                    'data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE.xlsx')
        shutil.copy('./test_data/task_today_UZB_SMALL_NEW/task_today_UZB_LARGE_items_processing.csv',
                    'data/sbkrzs/download/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_LARGE_items_processing.csv')

        result, barcodes, messages = task.check_form_file()

        self.assertTrue(result, 'Good file should be valid')
        self.assertEqual(len(barcodes), 2, 'Processing file should have 2 barcodes')

    def test_new_task_1(self):
        sftp.put('./test_data/test_data.xlsx',
                 f'./sbkzhk/upload/task_{date.today().isoformat()}_ZHK_SMALL.xlsx')
        self.assertTrue(sftp.is_file(f'./sbkzhk/upload/task_{date.today().isoformat()}_ZHK_SMALL.xlsx'),
                        'Task file should be uploaded')

        new_tasks = speibi.RemoteLocation().get_new_tasks()

        self.assertGreater(len(new_tasks), 0, 'New tasks should be available')

        for new_task_path in new_tasks:
            new_task = speibi.NewTask(new_task_path)

        self.assertTrue(sftp.is_file(
            f'./sbkzhk/download/storage_tasks/task_{date.today().isoformat()}_ZHK_SMALL_NEW/task_{date.today().isoformat()}_ZHK_SMALL.xlsx'),
                        'Task file should be uploaded')

        sftp.put('./test_data/test_data.xlsx',
                 f'./sbkzhk/upload/task_{date.today().isoformat()}_ZHK_SMALL_DELETE.xlsx')

        new_tasks = speibi.RemoteLocation().get_new_tasks()

        for new_task_path in new_tasks:
            new_task = speibi.NewTask(new_task_path)

        self.assertFalse(sftp.is_file(f'./sbkzhk/upload/task_{date.today().isoformat()}_ZHK_SMALL_DELETE.xlsx'),
                         'Task source file should be deleted after task deletion')

        self.assertFalse(sftp.is_file(f'./sbkzhk/download/storage_tasks/task_{date.today().isoformat()}_ZHK_SMALL_NEW/task_{date.today().isoformat()}_ZHK_SMALL.xlsx'),
                         'Task should be deleted after task deletion')

    def test_new_task_2(self):
        sftp.put('./test_data/test_data.xlsx',
                 f'./sbkzbz/upload/task_{date.today().isoformat()}_ZBZ_LARGE.xlsx')
        self.assertTrue(sftp.is_file(f'./sbkzbz/upload/task_{date.today().isoformat()}_ZBZ_LARGE.xlsx'),
                        'Task file should be uploaded')

        new_tasks = speibi.RemoteLocation().get_new_tasks()
        for new_task_path in new_tasks:
            speibi.NewTask(new_task_path)

        workflow.start('LARGE')

        self.assertTrue(sftp.is_dir(f'./sbkzbz/download/storage_tasks/task_{date.today().isoformat()}_ZBZ_LARGE_DONE'),
                        'Task should be done')

        sftp.put('./test_data/test_data.xlsx',
                 f'./sbkzbz/upload/task_{date.today().isoformat()}_{(date.today() + timedelta(days=2)).isoformat()}_ZBZ_LARGE_RESTART.xlsx')

        new_tasks = speibi.RemoteLocation().get_new_tasks()
        for new_task_path in new_tasks:
            speibi.NewTask(new_task_path)

        self.assertTrue(sftp.is_dir(f'./sbkzbz/download/storage_tasks/task_{(date.today() + timedelta(days=2)).isoformat()}_ZBZ_SMALL_NEW'),
                        'Task should be new')
        workflow.start('SMAll')
        self.assertTrue(sftp.is_dir(f'./sbkzbz/download/storage_tasks/task_{(date.today() + timedelta(days=2)).isoformat()}_ZBZ_SMALL_READY'),
                        'Task should be ready')


    @classmethod
    def tearDownClass(cls):
        sftp.close()
