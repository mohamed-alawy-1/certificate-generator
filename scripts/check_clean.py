import sys, types, os

# Ensure project root is in sys.path so 'app' can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Stubs so we can import app without installing all deps
sys.modules['eventlet'] = types.SimpleNamespace(monkey_patch=lambda: None)
class DummyFlask:
    def __init__(self, *a, **k):
        self.config = {}
    def route(self, *a, **k):
        def decorator(f):
            return f
        return decorator
    def before_first_request(self, *a, **k):
        def decorator(f):
            return f
        return decorator
    def run(self, *a, **k):
        return None
flask = types.SimpleNamespace(
    Flask=DummyFlask,
    render_template=lambda *a, **k: '',
    jsonify=lambda *a, **k: {},
    request=types.SimpleNamespace()
)
sys.modules['flask'] = flask

class DummySocketIO:
    def __init__(self, *a, **k):
        pass
    def emit(self, *a, **k):
        return None
    def on(self, *args, **k):
        def decorator(f):
            return f
        return decorator

sys.modules['flask_socketio'] = types.SimpleNamespace(SocketIO=DummySocketIO, emit=lambda *a, **k: None)

from app import clean_name

cases = [
    'احمد محمد محمود',
    'احمد محمد احمد',
    'احمد محمد محمود السيد', # El Sayed at end should STAY
    'السيد احمد محمد',     # El Sayed at start should GO
    'شيخه محمد',
    'د دعاء حبيب',
    'محمد مهندس',          # 'mohandes' at end should STAY (if it's part of name, though unlikely. But safe standard)
    'المهندس احمد',        # 'El Mohandes' at start should GO
    'ا. احمد',
    'أ. احمد',
    'خالد محمود',
    'أحمد محمد أحمد السيد المنياوى', # El Sayed in middle/end should STAY
]

print("-" * 60)
print(f"{'Original':<30} | {'Cleaned':<30}")
print("-" * 60)
for s in cases:
    cleaned = clean_name(s)
    print(f"{s:<30} | {cleaned:<30}")
print("-" * 60)
