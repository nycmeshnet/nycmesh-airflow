from airflow.models import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

def generate_certbot_tsig_cert(fqdn_string, dns_server, tsig_key_name, tsig_key, full_chain_path, priv_key_path):
    tsig_ini_file_path = "/tsig.ini"
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
        "--fullchain-path",
        full_chain_path,
        "--key-path",
        priv_key_path,
    ], check=True)
    
    print(completed.stdout)
    print(completed.stderr)
    
    Path(tsig_ini_file_path).unlink()

    return full_chain_path, priv_key_path

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
    
args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

with DAG(
    dag_id="omni_nn_certv1",
    default_args=args,
    schedule_interval=None,
    tags=["james", "nn", "tsig", "cert", "device"]
) as dag:
    @task.virtualenv(
        task_id="certbot_omni_nn_certv1", requirements=["certbot", "paramiko==3.5.0", "scp==0.15.0", "apache-airflow==2.10.5"], system_site_packages=False
    )
    def omni_nn_cert_task():
        # Imports inside of the virtual environment
        from paramiko import SSHClient, AutoAddPolicy
        from scp import SCPClient

        import subprocess
        from pathlib import Path
        from airflow.models import Variable
        
        tmp = subprocess.run("printenv", check=True)
        print(tmp.stdout)
        print(comptmpleted.stderr)
        
        in_scope_nn = ["592"]

        DNS_SERVER = Variable.get("tsigdns")
        TSIG_KEY_NAME = Variable.get("tsigkeyname")
        TSIG_KEY = Variable.get("tsigkey")
        
        for nn in in_scope_nn:
            print(f"Getting cert for {nn}")
            cert_path = f"/fullchain{nn}.pem"
            priv_key_path = f"/privkey{nn}.pem"
            fqdn = f"{nn}.nn.mesh.nycmesh.net"
            generate_certbot_tsig_cert(fqdn, DNS_SERVER, TSIG_KEY_NAME, TSIG_KEY, cert_path, priv_key_path)
            deploy_to_omni(fqdn, Variable.get("omni"), cert_path, priv_key_path)
            print("Finished")
    
    omni_nn_cert_task()
