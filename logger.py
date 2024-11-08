class Logger():
    
    def __init__(self):
        self.logs = []
    
    
    def log_info(self,
        subject,
        abs_time,
        type="info",
        info="",
        value=0
    ):
        log = {
            "subject": subject,
            "abs_time": abs_time,
            "type": type,
            "info": info,
            "value": value
        }
        self.logs.append(log)
        