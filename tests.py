import unittest
from unittest.mock import patch, MagicMock
from install_postgres import RemotePostgresInstaller


class TestRemotePostgresInstaller(unittest.TestCase):

    @patch('paramiko.SSHClient')
    def test_connect_success(self, MockSSHClient):
        mock_client = MagicMock()
        MockSSHClient.return_value = mock_client

        installer = RemotePostgresInstaller(hostname='192.168.1.1', username='root', ssh_key_path='/path/to/key')
        installer.client = mock_client
        installer.connect()
        mock_client.connect.assert_called_with('192.168.1.1', username='root', key_filename='/path/to/key')

    @patch('paramiko.SSHClient')
    def test_run_command(self, MockSSHClient):
        mock_client = MagicMock()
        MockSSHClient.return_value = mock_client

        installer = RemotePostgresInstaller(
            hostname='192.168.1.1',
            username='root',
            ssh_key_path='/path/to/key'
        )
        installer.client = mock_client

        mock_stdout = MagicMock()
        mock_stdout.read.return_value = b""
        mock_stderr = MagicMock()
        mock_stderr.read.return_value = "Ошибка выполнения".encode("utf-8")
        mock_stderr.channel.recv_exit_status.return_value = 1

        mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)

        output, error = installer.run_command("echo 'Test command'")

        self.assertEqual(output, "")
        self.assertEqual(error, "Ошибка выполнения")
        mock_client.exec_command.assert_called_with("echo 'Test command'")

        with self.assertLogs(level='INFO') as log:
            installer.run_command("echo 'Test command'")
            self.assertIn("Выполнение команды: echo 'Test command'", log.output[0])
            self.assertIn("Ошибка выполнения: Ошибка выполнения", log.output[1])

    @patch('paramiko.SSHClient')
    def test_check_load(self, MockSSHClient):
        mock_client = MagicMock()
        MockSSHClient.return_value = mock_client
        mock_stdout = MagicMock()
        mock_stdout.read.return_value = b"0.1"
        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b""
        mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)

        installer = RemotePostgresInstaller(hostname='192.168.1.1', username='root', ssh_key_path='/path/to/key')
        installer.client = mock_client
        load = installer.check_load()

        self.assertEqual(load, 0.1)

    @patch('paramiko.SSHClient')
    def test_detect_os(self, MockSSHClient):
        mock_client = MagicMock()
        MockSSHClient.return_value = mock_client
        mock_stdout = MagicMock()
        mock_stdout.read.return_value = b"ubuntu"
        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b""
        mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)
        installer = RemotePostgresInstaller(hostname='192.168.1.1', username='root', ssh_key_path='/path/to/key')
        installer.client = mock_client
        os_check = installer.detect_os()
        self.assertEqual(os_check, "ubuntu")

    @patch('paramiko.SSHClient')
    def test_install_postgres(self, MockSSHClient):
        mock_client = MagicMock()
        MockSSHClient.return_value = mock_client

        mock_stdout_os = MagicMock()
        mock_stdout_os.read.return_value = b"ubuntu"

        mock_stdout_install = MagicMock()
        mock_stdout_install.read.return_value = "PostgreSQL установлен".encode("utf-8")

        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b""

        mock_client.exec_command.side_effect = [
            (None, mock_stdout_os, mock_stderr),
            (None, mock_stdout_install, mock_stderr),
        ]

        installer = RemotePostgresInstaller(hostname='192.168.1.1', username='root', ssh_key_path='/path/to/key')
        installer.client = mock_client
        output, error = installer.install_postgres()

        self.assertIn("PostgreSQL установлен", output)
        self.assertEqual(error, "")

    @patch('paramiko.SSHClient')
    def test_configure_postgres_debian(self, MockSSHClient):
        mock_client = MagicMock()
        MockSSHClient.return_value = mock_client

        installer = RemotePostgresInstaller(
            hostname='192.168.1.1',
            username='root',
            ssh_key_path='/path/to/key'
        )
        installer.client = mock_client

        installer.detect_os = MagicMock(return_value='debian')
        installer.run_command = MagicMock()
        installer.check_postgres_connection = MagicMock()

        installer.configure_postgres()

        config_path = "/etc/postgresql/15/main"

        installer.run_command.assert_any_call(
            f"""sed -i "s/^#listen_addresses = 'localhost'/listen_addresses = '*'/" {config_path}/postgresql.conf"""
        )
        installer.run_command.assert_any_call(
            f"""grep -qxF "host all student 0.0.0.0/0 md5" {config_path}/pg_hba.conf || echo "host all student 192.168.1.42/32 md5" >> {config_path}/pg_hba.conf"""
        )
        installer.run_command.assert_any_call(
            """sudo -u postgres psql -c "ALTER USER student WITH PASSWORD 'your_password';" """
        )
        installer.run_command.assert_any_call("systemctl restart postgresql")

        installer.check_postgres_connection.assert_called_with('192.168.1.1')

    from unittest.mock import patch, MagicMock

    @patch('paramiko.SSHClient')
    def test_configure_postgres_almalinux(self, MockSSHClient):
        mock_client = MagicMock()
        MockSSHClient.return_value = mock_client

        installer = RemotePostgresInstaller(
            hostname='192.168.1.1',
            username='root',
            ssh_key_path='/path/to/key'
        )
        installer.client = mock_client

        installer.detect_os = MagicMock(return_value='almalinux')
        installer.run_command = MagicMock()
        installer.check_postgres_connection = MagicMock()

        installer.configure_postgres()

        config_path = "/var/lib/pgsql/data"

        installer.run_command.assert_any_call(
            "test -f /var/lib/pgsql/data/PG_VERSION || postgresql-setup --initdb"
        )
        installer.run_command.assert_any_call(
            f"""grep -qxF "host all student 0.0.0.0/0 md5" {config_path}/pg_hba.conf || echo "host all student 192.168.1.42/32 md5" >> {config_path}/pg_hba.conf"""
        )
        installer.run_command.assert_any_call(
            """sudo -u postgres psql -c "ALTER USER student WITH PASSWORD 'your_password';" """
        )
        installer.run_command.assert_any_call("systemctl restart postgresql")

        installer.check_postgres_connection.assert_called_with('192.168.1.1')

    @patch('paramiko.SSHClient')
    def test_check_postgres_connection(self, MockSSHClient):
        mock_client = MagicMock()
        MockSSHClient.return_value = mock_client
        mock_client.exec_command.return_value = (MagicMock(), MagicMock(), MagicMock())
        mock_client.exec_command.return_value[0].read.return_value = b"1"

        installer = RemotePostgresInstaller(hostname='192.168.1.1', username='root', ssh_key_path='/path/to/key')
        installer.client = mock_client
        installer.check_postgres_connection('192.168.1.1')
        mock_client.exec_command.assert_called()

    @patch('paramiko.SSHClient')
    def test_check_connection_success(self, MockSSHClient):
        mock_client = MagicMock()
        MockSSHClient.return_value = mock_client

        mock_stdout = MagicMock()
        mock_stdout.read.return_value = b"SELECT 1\n ?column?\n----------\n        1\n(1 row)\n"

        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b""

        mock_client.exec_command.return_value = (None, mock_stdout, mock_stderr)

        installer = RemotePostgresInstaller(hostname='192.168.1.1', username='root', ssh_key_path='/path/to/key')
        installer.client = mock_client
        output, error = installer.check_connection()

        self.assertIn("1 row", output)
        self.assertEqual(error, "")
        mock_client.exec_command.assert_called()


if __name__ == "__main__":
    unittest.main()
