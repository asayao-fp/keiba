import win32com.client

jv = win32com.client.Dispatch("JVDTLab.JVLink")

print("JVSetUIProperties:", jv.JVSetUIProperties())

candidates = ["", "UNKNOWN", "TEST"]
for sid in candidates:
    try:
        rc = jv.JVInit(sid)
        print(f"JVInit({sid!r}) = {rc}")
    except Exception as e:
        print(f"JVInit({sid!r}) raised: {e}")