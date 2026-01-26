from extras.scripts import Script
import os
import requests
import subprocess
import tempfile
from pathlib import Path

class DeployHuaweiWithGitLab(Script):
    class Meta:
        name = "Deploy Huawei Config with GitLab Control"
        description = "Generates rendered config from NetBox and commits to GitLab for version control and manual deployment"
        commit_default = True
    
    def get_rendered_config(self) -> str:
        """Obtiene configuración renderizada desde NetBox (igual que tu script original)"""
        NETBOX_URL = "http://192.168.117.135:8000"
        NETBOX_TOKEN = "c889397e6b09cfd1556378047213220b2c47b7e8"
        DEVICE_ID = 3
        
        url = f"{NETBOX_URL}/api/dcim/devices/{DEVICE_ID}/render-config/"
        headers = {
            "Authorization": f"Token {NETBOX_TOKEN}",
            "Accept": "text/plain",
        }
        
        self.log_info("📡 Solicitando configuración renderizada a NetBox...")
        resp = requests.post(url, headers=headers)
        resp.raise_for_status()
        
        # Verificación adicional: asegurarse de que no es JSON
        if resp.text.strip().startswith("{"):
            raise RuntimeError("❌ ¡Se recibió JSON! Algo está mal en la solicitud.")
        
        return resp.text
    
    def commit_to_gitlab(self, config_text: str, user: str):
        """Commitea la configuración a GitLab"""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                os.chdir(temp_dir)
                
                # Clonar repositorio GitLab
                GITLAB_REPO = "https://oauth2:TU_GITLAB_ACCESS_TOKEN@gitlab.com/tu-usuario/network-automation.git"
                subprocess.run(["git", "clone", GITLAB_REPO, "."], check=True, capture_output=True)
                
                # Crear directorio y guardar configuración
                configs_dir = Path("configs")
                configs_dir.mkdir(exist_ok=True)
                config_file = configs_dir / "huawei-router.cfg"
                
                with open(config_file, 'w') as f:
                    f.write(config_text)
                
                # Verificar si hay cambios
                result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
                
                if result.stdout.strip():
                    # Configurar git y commitear
                    subprocess.run(["git", "config", "user.email", "netbox@automation.local"])
                    subprocess.run(["git", "config", "user.name", "NetBox Automation"])
                    subprocess.run(["git", "add", str(config_file)])
                    commit_msg = f"Full config update - {user}"
                    subprocess.run(["git", "commit", "-m", commit_msg])
                    subprocess.run(["git", "push", "origin", "master"])
                    
                    self.log_success("✅ Configuración commiteada a GitLab exitosamente")
                    self.log_info("🔄 Aprobación manual requerida en pipeline de GitLab")
                else:
                    self.log_info("ℹ️ Sin cambios detectados, no se realizó commit")
                    
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode() if e.stderr else str(e)
            self.log_failure(f"❌ Error Git: {error_msg}")
            raise
        except Exception as e:
            self.log_failure(f"❌ Error inesperado: {e}")
            raise
    
    def run(self, data, commit):
        try:
            user = str(data.get('user', 'system'))
            
            # Paso 1: Obtener configuración renderizada (tu lógica original)
            config_text = self.get_rendered_config()
            
            if not config_text.strip():
                self.log_warning("⚠️ Configuración renderizada está vacía")
                return
            
            # Paso 2: Commitear a GitLab (nueva funcionalidad)
            self.commit_to_gitlab(config_text, user)
            
        except Exception as e:
            self.log_failure(f"❌ Error en el proceso: {e}")
