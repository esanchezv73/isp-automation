from extras.scripts import Script
import requests
import base64

class DeployHuaweiWithGitLabAPI(Script):
    class Meta:
        name = "Deploy Huawei Config via GitLab API"
        description = "Sends rendered config to GitLab via API for version control and manual deployment"
        commit_default = True
    
    def get_rendered_config(self) -> str:
        """Obtiene configuración renderizada desde NetBox"""
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
        
        if resp.text.strip().startswith("{"):
            raise RuntimeError("❌ ¡Se recibió JSON! Algo está mal en la solicitud.")
        
        return resp.text
    
    def update_file_in_gitlab(self, config_content: str, user: str):
        """Actualiza el archivo existente en GitLab"""
        try:
            # Configuración
            GITLAB_API_URL = "https://gitlab.com/api/v4"
            PROJECT_ID = "77963658"
            GITLAB_ACCESS_TOKEN = "glpat-LqRUom1qiIjcXFlhVlj_xG86MQp1OmpycHliCw.01.1210bogtr"
            FILE_PATH = "configs/huawei-router.cfg"
            BRANCH = "master"
            
            # URL con codificación correcta
            file_url = f"{GITLAB_API_URL}/projects/{PROJECT_ID}/repository/files/configs%2Fhuawei-router.cfg"
            
            headers = {
                "PRIVATE-TOKEN": GITLAB_ACCESS_TOKEN,
                "Content-Type": "application/json"
            }
            
            # Obtener contenido actual para comparar
            params = {"ref": BRANCH}
            resp = requests.get(file_url, headers=headers, params=params)
            
            if resp.status_code == 200:
                current_content = base64.b64decode(resp.json()['content']).decode('utf-8')
                if current_content.strip() == config_content.strip():
                    self.log_info("ℹ️ Sin cambios detectados")
                    return
            
            # Actualizar archivo (usamos PUT porque ya existe)
            payload = {
                "branch": BRANCH,
                "content": config_content,
                "commit_message": f"Full config update - {user}"
            }
            
            resp = requests.put(file_url, headers=headers, json=payload)
            resp.raise_for_status()
            
            self.log_success("✅ Configuración actualizada en GitLab exitosamente")
            self.log_info("🔄 Aprobación manual requerida en pipeline de GitLab")
            
        except Exception as e:
            self.log_failure(f"❌ Error: {e}")
            raise
    
    def run(self, data, commit):
        try:
            user = str(data.get('user', 'system'))
            config_text = self.get_rendered_config()
            
            if not config_text.strip():
                self.log_warning("⚠️ Configuración renderizada está vacía")
                return
            
            self.update_file_in_gitlab(config_text, user)
            
        except Exception as e:
            self.log_failure(f"❌ Error en el proceso: {e}")
