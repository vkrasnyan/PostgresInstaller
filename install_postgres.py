import paramiko
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RemotePostgresInstaller:
    def __init__(self, hostname, username, ssh_key_path):
        self.hostname = hostname
        self.username = username
        self.ssh_key_path = ssh_key_path
        self.client = None

    def connect(self):
        """Устанавливает SSH-соединение"""
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.client.connect(self.hostname, username=self.username, key_filename=self.ssh_key_path)
            logger.info(f"Успешное подключение к {self.hostname}")
        except Exception as e:
            logger.error(f"Ошибка подключения к {self.hostname}: {e}")
            raise

    def run_command(self, command):
        """Выполняет команду на удаленном сервере"""
        logger.info(f"Выполнение команды: {command}")
        stdin, stdout, stderr = self.client.exec_command(command)
        output, error = stdout.read().decode(), stderr.read().decode()
        if error:
            logger.error(f"Ошибка выполнения: {error}")
        return output, error

    def check_load(self):
        """Получает среднюю загрузку сервера"""
        load, _ = self.run_command("cat /proc/loadavg | awk '{print $1}'")
        logger.info(f"Средняя загрузка: {load.strip()}")
        return float(load.strip())

    def install_postgres(self):
        """Устанавливает PostgreSQL в зависимости от ОС"""
        os_check, _ = self.run_command("cat /etc/os-release | grep '^ID=' | cut -d'=' -f2")
        os_id = os_check.strip().lower()

        if os_id in ['debian', 'ubuntu']:
            install_cmd = "DEBIAN_FRONTEND=noninteractive apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql"
        elif os_id in ['centos', 'almalinux', 'rhel']:
            install_cmd = "dnf install -y postgresql-server postgresql-contrib && postgresql-setup --initdb && systemctl enable postgresql"
        else:
            logger.error(f"ОС {os_id} не поддерживается")
            raise ValueError(f"ОС {os_id} не поддерживается")

        output, error = self.run_command(install_cmd)
        logger.info("PostgreSQL установлен")
        return output, error

    def configure_postgres(self):
        """Настраивает PostgreSQL для внешних подключений и ограничивает доступ пользователя"""
        self.run_command(
            """sed -i "s/^#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/15/main/postgresql.conf"""
        )
        self.run_command("""grep -qxF "host all student 192.168.1.2/32 md5" /etc/postgresql/15/main/pg_hba.conf || echo "host all student 192.168.1.2/32 md5" >> /etc/postgresql/15/main/pg_hba.conf""")
        self.run_command("systemctl restart postgresql")
        logger.info("PostgreSQL настроен")

    def check_connection(self):
        """Проверяет доступность PostgreSQL"""
        output, error = self.run_command("""sudo -u postgres bash -c 'cd ~ && psql -c "SELECT 1;"'""")
        if "1 row" in output:
            logger.info("PostgreSQL успешно отвечает на запросы")
        else:
            logger.error("Ошибка проверки подключения к PostgreSQL")
        return output, error

    def close(self):
        """Закрывает SSH-соединение"""
        if self.client:
            self.client.close()
            logger.info("SSH-соединение закрыто")


if __name__ == "__main__":
    hostname = "83.222.25.151"
    username = "root"
    ssh_key_path = "/home/vickie/PycharmProjects/PostgreSQL_install/postgresql"

    installer = RemotePostgresInstaller(hostname, username, ssh_key_path)
    installer.connect()

    load = installer.check_load()

    logger.info("Начинаем установку PostgreSQL")
    installer.install_postgres()

    logger.info("Настраиваем PostgreSQL")
    installer.configure_postgres()

    logger.info("Проверяем подключение к PostgreSQL")
    installer.check_connection()

    installer.close()
