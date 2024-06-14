import unittest
import sftp.sftp as sftpmodule
import dotenv
import os
import shutil
import speibiutils.speibiutils as speibi
import speibiutils.workflow as workflow
from datetime import date

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
            os.mkdir(f'./data/{directory}/upload')
            os.mkdir(f'./data/{directory}/upload/storage_tasks')

            # Clean remote directories
            if sftp.is_dir(f'./{directory}'):
                sftp.rmtree(f'./{directory}')
            sftp.mkdir(f'./{directory}')
            sftp.mkdir(f'./{directory}/upload')
            sftp.mkdir(f'./{directory}/upload/storage_tasks')

    def test_get_remote_directories(self):

        remote_path = './sbkrzs/upload/storage_tasks/task_2041-01-01_UBS_LARGE_NEW'
        sftp.mkdir(remote_path)
        directories = speibi.RemoteLocation().directories
        self.assertTrue('task_2041-01-01_UBS_LARGE_NEW' in directories, 'Remote directory should be available')
        self.assertTrue(sftp.is_dir(remote_path), 'Remote directory not created')

        # sftp.remove('./sbkrzs/upload/storage_tasks/task_2041-01-01_UBS_LARGE_NEW')

    def test_tasks_summary_save(self):
        speibi.TaskSummary().save()
        self.assertTrue(sftp.is_file('./sbkubs/upload/storage_tasks/task_summary.xlsx'),
                        'task_summary.xlsx not saved')

    def test_clean_local_directories(self):

        os.mkdir('./data/sbkzbs/upload/storage_tasks/task_2000-01-01_ZBS_LARGE_NEW')
        os.mkdir(f'./data/sbkzbs/upload/storage_tasks/task_{date.today().isoformat()}_ZBS_LARGE_NEW')

        speibi.TaskSummary().clean_local_directories()

        self.assertFalse(os.path.exists('./data/sbkzbs/upload/storage_tasks/task_2000-01-01_ZBS_LARGE_NEW'),
                         'Old local directory should be deleted')
        self.assertTrue(
            os.path.exists(f'./data/sbkzbs/upload/storage_tasks/task_{date.today().isoformat()}_ZBS_LARGE_NEW'),
            'New local directory should be kept')

        os.rmdir(f'./data/sbkzbs/upload/storage_tasks/task_{date.today().isoformat()}_ZBS_LARGE_NEW')

    def test_get_task_parameters(self):
        sftp.mkdir('./sbkrzs/upload/storage_tasks/task_2032-01-01_RZS_LARGE_NEW')
        remote = speibi.RemoteLocation()
        t = speibi.TaskSummary()
        remote_path = [p for p in remote.paths if 'task_2032-01-01_RZS_LARGE_NEW' in p][0]
        task = speibi.Task(remote_path)
        parameters = task.get_parameters()
        self.assertEqual(parameters['Account'], 'sbkrzs', 'Account not correct, must be sbkrzs')
        self.assertEqual(parameters['Scheduled_date'], '2032-01-01', 'Account not correct, must be sbkrzs')
        # sftp.remove('./sbkrzs/upload/storage_tasks/task_2032-01-01_RZS_LARGE_NEW')

    def test_clean_remote_directories(self):
        sftp.mkdir('./sbkrzs/upload/storage_tasks/task_2033-01-01_RZS_LARGE_NEW')
        sftp.mkdir('./sbkrzs/upload/storage_tasks/task_2033-01-01_RZS_LARGE_NEW')
        sftp.mkdir('./sbkzbs/upload/storage_tasks/task_2033-01-01_ZBS_LARGE_NEW')
        sftp.mkdir('./sbkrzs/upload/storage_tasks/task_2034-01-01_RZS_LARGE_NEW')
        sftp.put('./test_data/test_data.xlsx',
                 './sbkrzs/upload/storage_tasks/task_2034-01-01_RZS_LARGE_NEW/task_2034-01-01_RZS.xlsx')

        speibi.TaskSummary().clean_remote_directories()
        directories = speibi.RemoteLocation().directories
        # self.assertEqual(len(directories), 3, 'Remote directories not cleaned')
        self.assertTrue('task_2034-01-01_RZS_LARGE_NEW' in directories, 'Valid directory not available')
        self.assertTrue('task_2033-01-01_RZS_LARGE_NEW' in directories, 'Valid directory not available')
        self.assertTrue('task_2033-01-01_ZBS_LARGE_ERROR' in directories, 'Error not detected')

        # sftp.remove('./sbkrzs/upload/storage_tasks/task_2034-01-01_RZS_LARGE_NEW')
        # sftp.remove('./sbkrzs/upload/storage_tasks/task_2033-01-01_RZS_LARGE_NEW')
        # sftp.remove('./sbkzbs/upload/storage_tasks/task_2033-01-01_ZBS_LARGE_ERROR')

    def test_check_forms_conformity(self):

        sftp.mkdir('./sbkuzh/upload/storage_tasks/task_2050-01-01_UZH_LARGE_NEW')
        sftp.put('./test_data/test_data.xlsx',
                 './sbkuzh/upload/storage_tasks/task_2050-01-01_UZH_LARGE_NEW/task_2050-01-01_UZH.xlsx')

        task_summary = speibi.TaskSummary()
        task_summary.clean_remote_directories()
        task_summary.clean_local_directories()
        task_summary.check_forms_conformity()

        self.assertTrue(sftp.is_dir('./sbkuzh/upload/storage_tasks/task_2050-01-01_UZH_LARGE_READY'),
                        'Valid task not created')
        self.assertTrue(sftp.is_file('./sbkuzh/upload/storage_tasks/task_2050-01-01_UZH_LARGE_READY/'
                                     'form_check_result.txt'),
                        'Feedback txt file not created')
        self.assertTrue(os.path.exists('./data/sbkuzh/upload/storage_tasks/task_2050-01-01_UZH_LARGE_READY/'
                                       'form_check_result.txt'),
                        'Local feedback txt file not created')

        sftp.rename('./sbkuzh/upload/storage_tasks/task_2050-01-01_UZH_LARGE_READY',
                    './sbkuzh/upload/storage_tasks/task_2050-01-01_UZH_LARGE_NEW')

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
        sftp.mkdir(f'./sbkhsg/upload/storage_tasks/task_{date.today().isoformat()}_HSG_SMALL_NEW')
        sftp.put('./test_data/test_data.xlsx',
                 f'./sbkhsg/upload/storage_tasks/task_{date.today().isoformat()}_HSG_SMALL_NEW/task_{date.today().isoformat()}_HSG.xlsx')
        workflow.start('SMALL')

        self.assertTrue(sftp.is_dir(f'./sbkhsg/upload/storage_tasks/task_{date.today().isoformat()}_HSG_SMALL_DONE'),
                        'Task should be done')

        task_summary = speibi.TaskSummary()
        self.assertTrue(task_summary.get_processing_task() is None, 'No task should be processing')
        self.assertFalse(task_summary.tasks.loc[(task_summary.tasks['Directory'] == f'task_{date.today().isoformat()}_HSG_SMALL_DONE') &
                                                (task_summary.tasks['State'] == 'DONE')].empty,
                    'Task should be done')

    def test_Task(self):
        t = speibi.Task(directory='task_2041-01-01_HSG_LARGE_NEW', account='sbkhsg')
        self.assertTrue(t.is_valid(), 'Provided task is valid')
        self.assertEqual(t.get_name(), 'task_2041-01-01_HSG', 'Task name should be task_2041-01-01_HSG')
        self.assertEqual(t.get_form_path(),
                         'sbkhsg/upload/storage_tasks/task_2041-01-01_HSG_LARGE_NEW/task_2041-01-01_HSG.xlsx',
                         'Form path should be sbkhsg/upload/storage_tasks/task_2041-01-01_HSG_LARGE_NEW/task_2041-01-01_HSG.xlsx')
        self.assertEqual(t.get_processing_file_path(local=True),
                         'data/sbkhsg/upload/storage_tasks/task_2041-01-01_HSG_LARGE_NEW/task_2041-01-01_HSG_items_processing.csv',
                         'Form path should be data/sbkhsg/upload/storage_tasks/task_2041-01-01_HSG_LARGE_NEW/task_2041-01-01_HSG_items_processing.csv')

        self.assertEqual(t.get_parameters()['Scheduled_date'], '2041-01-01', 'Scheduled date should be 2041-01-01')
        self.assertEqual(t.get_parameters()['State'], 'NEW', 'State should be NEW')
        self.assertEqual(t.get_parameters()['Account'], 'sbkhsg', 'Account should be sbkhsg')

    def test_check_excel_file_conformity(self):

        # Valid form
        os.mkdir('data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW')
        shutil.copy('./test_data/test_data.xlsx',
                 'data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS.xlsx')

        task = speibi.Task('sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW')

        result, barcodes, messages = task.check_form_file()
        self.assertTrue(result, 'Valid file should be valid')
        self.assertTrue(len(barcodes) > 0, 'Barcodes should not be empty')

        os.remove('data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS.xlsx')

        # Invalid form: bad tab
        shutil.copy('./test_data/test_data_bad1.xlsx',
                 'data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS.xlsx')

        result, barcodes, messages = task.check_form_file()

        self.assertTrue(
            "Bad or missing sheet names, must be ['General', 'Items', 'Locations_mapping', 'Item_policies_mapping', 'data_validation']" in messages,
            'Error message not found (tabs error)')
        self.assertTrue(len(barcodes) == 0, 'Barcodes should be empty')

        os.remove('data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS.xlsx')

        # Invalid form: duplicate barcode in same form
        shutil.copy('./test_data/test_data_bad2.xlsx',
                 'data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS.xlsx')

        result, barcodes, messages = task.check_form_file()

        self.assertFalse(result, 'Bad file should not be valid')
        self.assertTrue("Duplicate barcodes in the file: {'A1001180331'}" in messages,
                        'Error message not found (duplicate barcodes error)')

        os.remove('data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS.xlsx')

        # Invalid form: duplicate barcode checking multiple files
        shutil.copy('./test_data/test_data.xlsx',
                 'data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS.xlsx')

        result, barcodes, messages = task.check_form_file(['A1001180331'])

        self.assertFalse(result, 'Bad file should not be valid')
        self.assertTrue("Barcodes {'A1001180331'} already in other tasks" in messages,
                        'Error message not found (barcode in other task)')

        os.remove('data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS.xlsx')

        # Check with processing file

        shutil.copy('./test_data/task_today_UZB_SMALL_NEW/task_today_UZB.xlsx',
                 'data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS.xlsx')
        shutil.copy('./test_data/task_today_UZB_SMALL_NEW/task_today_UZB_items_processing.csv',
                 'data/sbkrzs/upload/storage_tasks/task_2055-01-01_RZS_LARGE_NEW/task_2055-01-01_RZS_items_processing.csv')

        result, barcodes, messages = task.check_form_file()

        self.assertTrue(result, 'Good file should be valid')
        self.assertEqual(len(barcodes), 2, 'Processing file should have 2 barcodes')

    @classmethod
    def tearDownClass(cls):
        sftp.close()
