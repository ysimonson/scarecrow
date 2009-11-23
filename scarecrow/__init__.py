import hashlib

class ScarecrowIdent(str): pass

def ident(name):
    if isinstance(name, ScarecrowIdent):
        return name
    else:
        return ScarecrowIdent(hashlib.md5(name).digest())