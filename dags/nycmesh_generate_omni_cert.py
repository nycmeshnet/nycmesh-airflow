import datetime
import sys
import os

from airflow.sdk import dag, task

args = {
    'owner': 'Airflow',
    'start_date': datetime.datetime(2021, 1, 1),
}

@dag(
    dag_id="omni_nn_certv1",
    default_args=args,
    catchup=False,
    tags=["james", "nn", "tsig", "cert", "device"]
)
def omni_cert_dag():
    @task
    def get_devices():
        in_scope_nn = [
            "592",
            "544",
        ]
        return in_scope_nn

    @task(
        task_id="certbot_omni_nn_certv1"
    )
    def omni_nn_cert_task(nn):

        def generate_certbot_tsig_cert(fqdn_string, dns_server, tsig_key_name, tsig_key):
            tsig_ini_file_path = "/tmp/tsig.ini"
            with open(tsig_ini_file_path, "w") as fd:
                fd.write(f"""# Target DNS server
dns_rfc2136_server = {dns_server}
# Target DNS port
dns_rfc2136_port = 53
# TSIG key name
dns_rfc2136_name = {tsig_key_name}
# TSIG key secret
dns_rfc2136_secret = {tsig_key}
# TSIG key algorithm
dns_rfc2136_algorithm = HMAC-SHA512
""")
            os.chmod(tsig_ini_file_path, 0o600)
            
            config_dir = "/tmp/certbot_config"
            # Get the cert from Let's Encrypt
            completed = subprocess.run([
                "certbot",
                "certonly",
                "--dns-rfc2136",
                "--dns-rfc2136-credentials",
                tsig_ini_file_path,
                "--non-interactive",
                "--agree-tos",
                "-m",
                "jameso@nycmesh.net",
                "-d",
                fqdn_string,
                "--config-dir",
                config_dir,
                "--work-dir",
                "/tmp/certbot_work",
                "--logs-dir",
                "/tmp/certbot_logs",
                "-v",
            ], check=True)
            
            print(completed.stdout)
            print(completed.stderr)
            
            Path(tsig_ini_file_path).unlink()

            return f"{config_dir}/live/{fqdn_string}/fullchain.pem", f"{config_dir}/live/{fqdn_string}/privkey.pem"

        def deploy_to_omni(ip, password, cert_path, priv_key_path):
            with SSHClient() as ssh:
                ssh.set_missing_host_key_policy(AutoAddPolicy())
                ssh.connect(ip, username="admin", password=password, timeout=10)

                with SCPClient(ssh.get_transport()) as scp:
                    scp.put(cert_path, "fullchain.pem")
                    scp.put(priv_key_path, "privkey.pem")

                stdin, stdout, stderr = ssh.exec_command(
                    "/certificate/import file-name=fullchain.pem name=LEfullchain trusted=no;"
                    "/certificate/import file-name=privkey.pem name=LEprivkey trusted=no;"
                    "/ip/service set www-ssl certificate=LEfullchain disabled=no tls-version=only-1.2 address=10.0.0.0/8,199.167.59.0/24,199.170.132.0/24"
                )
                print(stdout.read())
                print(stderr.read())

        # Imports inside of the virtual environment
        from paramiko import SSHClient, AutoAddPolicy
        from scp import SCPClient

        import subprocess
        from pathlib import Path
        from airflow.models import Variable

        DNS_SERVER = "199.170.132.47"
        TSIG_KEY_NAME = "nn.mesh.nycmesh.net"
        TSIG_KEY = Variable.get("Airflow_tsigkey")

        print(f"Getting cert for {nn}")
        fqdn = f"{nn}.nn.mesh.nycmesh.net"
        fullchain_path, privkey_path = generate_certbot_tsig_cert(fqdn, DNS_SERVER, TSIG_KEY_NAME, TSIG_KEY)
        deploy_to_omni(fqdn, Variable.get("Airflow_omni"), fullchain_path, privkey_path)
        print("Finished")
    
    nns = get_devices()
    omni_nn_cert_task.expand(nn=nns)

omni_cert_dag()
