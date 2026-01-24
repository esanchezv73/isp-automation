from extras.scripts import Script
from dcim.models import Device
import subprocess
import logging

class ApplyBGPChanges(Script):
    class Meta:
        name = "Apply BGP Changes to Huawei Router"
        description = "Applies BGP policy changes to Huawei router"
        commit_default = True
    
    def run(self, data, commit):
        try:
            # Ejecutar script de Nornir para aplicar cambios incrementales
            result = subprocess.run(
                ['python3', '/root/automation/apply_bgp_policies.py'],
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.returncode == 0:
                self.log_success("✅ BGP policies applied successfully")
                self.log_info(f"Output: {result.stdout}")
            else:
                self.log_failure(f"❌ Failed to apply BGP policies: {result.stderr}")
                
        except Exception as e:
            self.log_failure(f"❌ Error applying BGP policies: {e}")
