import paramiko
import stat
import os
import logging
# from typing import Optional
# import sys


# class LogFile:
#     def __init__(self, file_name: Optional[str] = "", task_name: Optional[str] = None):
#         self.logger = self.config_log()
#
#     @staticmethod
#     def config_log(self) -> logging.Logger:
#         """Set the log configuration for the entire process
#
#         :param file_name: name of the log file
#         :return: None
#         """
#
#         # Create the log directory if it doesn't exist
#
#         log_path = f'./data'
#
#         if os.path.isdir(log_path) is False:
#             os.mkdir(log_path)
#
#         message_format = "%(asctime)s - %(levelname)s - %(message)s"
#         log_file_name = f'{log_path}/log.txt'
#         logging.basicConfig(format=message_format,
#                             level=logging.INFO,
#                             handlers=[logging.FileHandler(log_file_name),
#                                       logging.StreamHandler(sys.stdout)])
#         return logging.getLogger()
#
#     @staticmethod
#     def close_log(self) -> None:
#         logging.shutdown()


class SFTP:
    """Class to manage SFTP connections

    Attributes
    ----------
    SFTP_Client : paramiko.SFTPClient
        SFTP client to manage the connection
    """
    def __init__(self, host, user, password):
        """Constructor of SFTP class

        Parameters
        ----------
        host : str
        user : str
        password : str
        """
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # Auto accept host key
        try:
            ssh_client.connect(hostname=host,
                               port=22,
                               username=user,
                               password=password)
            self.SFTP_Client = ssh_client.open_sftp()

        except Exception as e:
            logging.error(f"Error connecting to SFTP: {e}")
            exit(1)

    def listdir(self, path):
        """List the contents of a directory

        Parameters
        ----------
        path : str
            Path of the directory to list"""
        contents = self.SFTP_Client.listdir(path)
        return contents

    def mkdir(self, path: str) -> None:
        """Create a directory in the remote server

        Parameters
        ----------
        path : str
            Path of the directory to create
        """
        try:
            self.SFTP_Client.stat(path)  # Test if remote_path exists
            logging.info(f"Directory {path} already exists")
            return
        except FileNotFoundError:
            pass
        try:
            self.SFTP_Client.mkdir(path)  # Test if remote_path exists
        except IOError as e:
            logging.error(f"Error creating directory {path}: {e}")

    def is_dir(self, path: str) -> bool:
        """Check if a path is a directory

        Parameters
        ----------
        path : str
            Path to check
        """
        try:
            f = self.SFTP_Client.lstat(path)
        except FileNotFoundError:
            return False
        return stat.S_ISDIR(f.st_mode)

    def is_file(self, path: str) -> bool:
        """Check if a path is a file

        Parameters
        ----------
        path : str
            Path to check
        """
        try:
            f = self.SFTP_Client.lstat(path)
        except FileNotFoundError:
            return False
        return stat.S_ISREG(f.st_mode)

    def is_path(self, path: str) -> bool:
        """Check if a path exists

        Parameters
        ----------
        path : str
            Path to check
        """
        try:
            _ = self.SFTP_Client.lstat(path)
        except FileNotFoundError:
            return False
        return True

    def remove(self, path: str) -> None:
        """Remove a file or directory in the remote server

        Parameters
        ----------
        path : str
            Path of the file or directory to remove
        """
        if self.is_dir(path) is True:
            try:
                self.SFTP_Client.rmdir(path)
            except IOError:
                logging.error(f"Error removing directory {path}")
        else:
            try:
                self.SFTP_Client.remove(path)
            except IOError:
                logging.error(f"Error removing file {path}")

    def rmtree(self, path: str) -> None:
        """Remove a directory and its contents in the remote server

        Parameters
        ----------
        path : str
            Path of the directory to remove
        """
        if self.is_path(path) is False:
            logging.error(f"Directory {path} not found")
            return
        if self.is_dir(path) is False:
            self.remove(path)
            return
        for f in self.listdir(path):
            f_path = path + '/' + f
            if self.is_dir(f_path) is True:
                self.rmtree(f_path)
            else:
                self.remove(f_path)
        self.remove(path)

    def put(self, local_path: str, remote_path: str) -> None:
        """Copy a file from local to remote server

        Parameters
        ----------
        local_path : str
            Path of the file to copy
        remote_path : str
            Path of the file in the remote server
        """
        try:
            self.SFTP_Client.put(local_path, remote_path)
        except IOError as e:
            logging.error(f"Error copying file {local_path} to {remote_path}: {e}")

    def get(self, remote_path: str, local_path: str) -> None:
        """Copy a file from remote to local server

        Parameters
        ----------
        remote_path : str
            Path of the file in the remote server
        local_path : str
            Path of the file to copy
        """
        try:
            self.SFTP_Client.get(remote_path, local_path)
        except IOError as e:
            logging.error(f"Error copying file {remote_path} to {local_path}: {e}")

    def copy_to_remote(self, local_path: str, remote_path: str) -> None:
        """Copy a file from local to remote server

        Parameters
        ----------
        local_path : str
            Path of the file to copy
        remote_path : str
            Path of the file in the remote server
        """
        if os.path.exists(local_path) is False:
            logging.error(f"Directory or file {local_path} not found")
            return

        if os.path.isdir(local_path) is False:
            self.put(local_path, remote_path)
        else:
            self.mkdir(remote_path)

            for local_entry in os.listdir(local_path):
                local_entry_path = local_path + "/" + local_entry
                remote_entry_path = remote_path + "/" + local_entry

                if os.path.isdir(local_entry_path):

                    self.mkdir(remote_entry_path)

                    self.copy_to_remote(local_entry_path, remote_entry_path)
                elif os.path.isfile(local_entry_path):
                    self.put(local_entry_path, remote_entry_path)

    def copy_to_local(self, remote_path: str, local_path: str) -> None:
        """Copy a file from remote to local server

        Parameters
        ----------
        remote_path : str
            Path of the file in the remote server
        local_path : str
            Path of the file to copy
        """
        if self.is_path(remote_path) is False:
            logging.error(f"Directory or file {remote_path} not found")
            return

        if self.is_dir(remote_path) is False:
            self.get(remote_path, local_path)
        else:
            try:
                os.mkdir(local_path)
            except OSError:
                pass

            for remote_entry in self.listdir(remote_path):
                remote_entry_path = remote_path + "/" + remote_entry
                local_entry_path = local_path + "/" + remote_entry

                if self.is_dir(remote_entry_path) is True:
                    try:
                        os.mkdir(local_entry_path)
                    except OSError:
                        pass
                    self.copy_to_local(remote_entry_path, local_entry_path)
                elif self.is_file(remote_entry_path) is True:
                    self.get(remote_entry_path, local_entry_path)

    def rename(self, old_path: str, new_path: str) -> None:
        """Rename a file or directory in the remote server

        Parameters
        ----------
        old_path : str
            Path of the file or directory to rename
        new_path : str
            New path of the file or directory
        """
        try:
            self.SFTP_Client.rename(old_path, new_path)
        except IOError as e:
            logging.error(f"Error renaming {old_path} to {new_path}: {e}")

    def close(self):
        self.SFTP_Client.close()


if __name__ == '__main__':
    pass
