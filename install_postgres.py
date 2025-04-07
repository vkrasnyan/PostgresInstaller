import paramiko
import logging
import sys

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

    def detect_os(self):
        """Определяет ID операционной системы"""
        os_check, _ = self.run_command("grep '^ID=' /etc/os-release | cut -d'=' -f2 | tr -d '\"'")
        return os_check.strip().lower()

    def install_postgres(self):
        """Устанавливает PostgreSQL в зависимости от ОС"""
        os_id = self.detect_os()

        if os_id in ['debian', 'ubuntu']:
            install_cmd = (
                "DEBIAN_FRONTEND=noninteractive apt-get update && "
                "DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql"
            )
        elif os_id in ['centos', 'almalinux', 'rhel']:
            install_cmd = (
                "dnf install -y postgresql-server postgresql-contrib && "
                "systemctl enable postgresql && "
                "systemctl start postgresql"
            )
        else:
            logger.error(f"ОС {os_id} не поддерживается")
            raise ValueError(f"ОС {os_id} не поддерживается")

        output, error = self.run_command(install_cmd)
        logger.info("PostgreSQL установлен")
        return output, error

    def configure_postgres(self):
        """Настраивает PostgreSQL для внешних подключений и ограничивает доступ пользователя"""
        os_id = self.detect_os()
        if os_id in ['debian', 'ubuntu']:
            config_path = "/etc/postgresql/15/main"
            self.run_command(
                f"""sed -i "s/^#listen_addresses = 'localhost'/listen_addresses = '*'/" {config_path}/postgresql.conf"""
            )
            self.run_command(f"""grep -qxF "host all student 192.168.1.2/32 md5" {config_path}/pg_hba.conf || echo "host all student 192.168.1.2/32 md5" >> {config_path}/pg_hba.conf""")
        elif os_id in ['centos', 'almalinux', 'rhel']:
            config_path = "/var/lib/pgsql/data"
            self.run_command("test -f /var/lib/pgsql/data/PG_VERSION || postgresql-setup --initdb")
            self.run_command(
                f"""sed -i "s/^#listen_addresses = 'localhost'.*/listen_addresses = '*'/" {config_path}/postgresql.conf"""
            )
            self.run_command(
                f"grep -qxF 'host all student 192.168.1.2/32 md5' {config_path}/pg_hba.conf || echo 'host all student 192.168.1.2/32 md5' >> {config_path}/pg_hba.conf")
        else:
            logger.error(f"ОС {os_id} не поддерживается")
            raise ValueError(f"ОС {os_id} не поддерживается")

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

    @staticmethod
    def choose_least_loaded_server(servers):
        """Выбирает сервер с наименьшей нагрузкой"""
        least_loaded_server = None
        min_load = float('inf')
        for server in servers:
            server.connect()
            load = server.check_load()
            if load < min_load:
                min_load = load
                least_loaded_server = server
            server.close()
        return least_loaded_server


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <ip1,ip2>")
        sys.exit(1)

    username = "root"
    ssh_key_path = "/home/vickie/PycharmProjects/PostgreSQL_install/postgresql"

    ip_list = sys.argv[1].split(",")
    servers = []

    for ip in ip_list:
        installer = RemotePostgresInstaller(ip.strip(), username, ssh_key_path)
        try:
            installer.connect()
            load = installer.check_load()
            servers.append((installer, load))
        except Exception as e:
            logger.error(f"Пропускаем сервер {ip} из-за ошибки: {e}")

    if not servers:
        logger.error("Нет доступных серверов")
        sys.exit(1)

    target_installer = sorted(servers, key=lambda x: x[1])[0][0]

    logger.info(f"Выбран сервер с наименьшей загрузкой: {target_installer.hostname}")

    logger.info("Установка PostgreSQL")
    target_installer.install_postgres()

    logger.info("Настройка PostgreSQL")
    target_installer.configure_postgres()

    logger.info("Проверка подключения к PostgreSQL")
    target_installer.check_connection()

    for installer, _ in servers:
        installer.close()
