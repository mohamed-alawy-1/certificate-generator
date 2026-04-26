#!/usr/bin/env python3
"""
Certificate Generator Dashboard v2
- Browse Google Drive folders
- Select sheets from folder
- Multiple variables support
"""

from flask import Flask, render_template, jsonify, request, redirect, url_for, session
from flask_socketio import SocketIO, emit
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import timedelta
from hmac import compare_digest
import threading
import time
import re
import json
import os
import glob
import logging
import secrets
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

app = Flask(__name__)
app.config['SECRET_KEY'] = (
    os.environ.get('APP_SECRET_KEY')
    or os.environ.get('FLASK_SECRET_KEY')
    or os.environ.get('SECRET_KEY')
    or secrets.token_hex(32)
)
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('SESSION_COOKIE_SECURE', '1') == '1'
try:
    session_timeout_minutes = max(30, int(os.environ.get('SESSION_TIMEOUT_MINUTES', '480')))
except (TypeError, ValueError):
    session_timeout_minutes = 480
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(
    minutes=session_timeout_minutes
)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Persistent file logger so app-level logs are visible in app.log as well.
APP_LOG_PATH = os.environ.get('APP_LOG_PATH', 'app.log')
app_logger = logging.getLogger('certificate_dashboard')
if not app_logger.handlers:
    file_handler = logging.FileHandler(APP_LOG_PATH, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    app_logger.addHandler(file_handler)
app_logger.setLevel(logging.INFO)
app_logger.propagate = False

# ============ STATE ============
state = {
    'status': 'idle',  # idle, running, paused, stopped, completed
    'total': 0,
    'completed': 0,
    'failed': 0,
    'current_name': '',
    'start_time': None,
    'logs': [],
    'retry_count': 0,  # Current retry attempt
    'max_retries': 1,  # Maximum number of retry attempts
    
    # Configuration
    'config': {
        'template_doc_url': '',
        'template_doc_id': '',
        'template_doc_name': '',
        'target_folder_url': '',
        'target_folder_id': '',
        'target_folder_name': '',
        'temp_folder_url': '',
        'temp_folder_id': '',
        'temp_folder_name': '',
        'sheet_url': '',
        'sheet_id': '',
        'sheet_name': '',
        'range_mode': 'all',  # 'all' or 'custom'
        'range_start': 2,
        'range_end': 1000,
        'link_column': 'O',
        'name_column': '',  # Auto-detected column with name
        'auto_watch': False,
        'watch_interval': 30,  # seconds
        'performance': {
            'enabled': True,
            'slow_threshold_sec': 6.0,
            'log_each_certificate': False
        },
        'cleanup': {
            'enabled': True,
            'remove_words': [
                # الألقاب العربية - استاذ بكل الأشكال
                'استاذ', 'استاذه', 'استاذة', 'أستاذ', 'أستاذه', 'أستاذة', 'إستاذ', 'إستاذه', 'إستاذة',
                'الاستاذ', 'الاستاذه', 'الاستاذة', 'الأستاذ', 'الأستاذه', 'الأستاذة', 'الإستاذ', 'الإستاذه', 'الإستاذة',
                'استاذ مساعد', 'أستاذ مساعد', 'إستاذ مساعد', 'استاذ مشارك', 'أستاذ مشارك', 'إستاذ مشارك',
                'استاذ مساعد دكتور', 'أستاذ مساعد دكتور', 'إستاذ مساعد دكتور',
                'استاذ اصول التربيه المساعده', 'أستاذ أصول التربية المساعدة', 'استاذ اصول التربية المساعدة',
                
                # ا. أ. بكل الأشكال
                'ا.', 'أ.', 'إ.', 'ا/', 'أ/', 'إ/', 'ا:', 'أ:', 'إ:',
                # Variants with underscore or dash (sometimes seen in sheets)
                'ا_', 'أ_', 'إ_', 'ا -', 'أ -', 'إ -', 'ا-', 'أ-', 'إ-',
                
                # دكتور
                'دكتور', 'دكتوره', 'دكتورة', 'الدكتور', 'الدكتوره', 'الدكتورة',
                'د.', 'د/', 'د:', 'Dr', 'Dr.', 'DR', 'DR.',
                
                # محامي ومهندس
                'محامي', 'محاميه', 'محامية', 'المحامي', 'المحاميه', 'المحامية', 'م.', 'م/', 'م:',
                'مهندس', 'مهندسه', 'مهندسة', 'المهندس', 'المهندسه', 'المهندسة', 'Eng', 'Eng.', 'ENG', 'ENG.',
                
                # م.د و م.م و أ.م.د
                'م.د', 'م.م', 'م.د.', 'م.م.', 'أ.م.د', 'أ.م.د.',
                
                # الطالب والطالبة
                'الطالب', 'الطالبه', 'الطالبة', 'طالب', 'طالبه', 'طالبة',
                
                # MR/MS بكل الأشكال
                'Mr', 'Mr.', 'MR', 'MR.', 'Mrs', 'Mrs.', 'MRS', 'MRS.',
                'Ms', 'Ms.', 'MS', 'MS.', 'Miss', 'MISS', 'miss',
                
                # سيد وسيدة
                'سيد', 'سيده', 'سيدة', 'السيد', 'السيده', 'السيدة',
                
                # المستشار والمساعد والمشارك
                'مستشار', 'مستشاره', 'مستشارة', 'المستشار', 'المستشاره', 'المستشارة',
                'مساعد', 'مساعده', 'مساعدة', 'المساعد', 'المساعده', 'المساعدة',
                'المساعد الاداري', 'المساعد الإداري', 'المساعدة الادارية', 'المساعدة الإدارية',
                'مشارك', 'مشاركه', 'مشاركة', 'المشارك', 'المشاركه', 'المشاركة',
                
                # الوكيل والوكيلة
                'وكيل', 'وكيله', 'وكيلة', 'الوكيل', 'الوكيله', 'الوكيلة',
                'الوكيل المساعد', 'الوكيلة المساعدة', 'الوكيل المساعد أ',
                
                # الاخصائي
                'اخصائي', 'اخصائيه', 'اخصائية', 'الاخصائي', 'الاخصائيه', 'الاخصائية',
                'أخصائي', 'أخصائيه', 'أخصائية', 'الأخصائي', 'الأخصائيه', 'الأخصائية',
                
                # المشرف التربوي
                'مشرف تربوي', 'مشرفه تربويه', 'مشرفة تربوية',
                'المشرف التربوي', 'المشرفه التربويه', 'المشرفة التربوية',
                
                # بروفيسور بكل الأشكال
                'بروفيسور', 'بروفسور', 'بروفيسوره', 'بروفيسورة', 'بروف', 'بروفه',
                'Prof', 'Prof.', 'PROF', 'PROF.', 'Professor', 'PROFESSOR',
                
                # شيخ وحاج
                'شيخ', 'الشيخ',
                'حاج', 'حاجه', 'حاجة', 'الحاج', 'الحاجه', 'الحاجة',
                
                # الرتب العسكرية
                'عميد', 'العميد', 'لواء', 'اللواء', 'عقيد', 'العقيد',
                'رائد', 'الرائد', 'نقيب', 'النقيب', 'ملازم', 'الملازم',
                
                # أخرى
                'قاضي', 'القاضي', 'كابتن', 'الكابتن', 'Captain', 'Capt', 'CAPT',
                'Sir', 'SIR', 'Madam', 'MADAM'
            ],
            'remove_before_slash': True,
            'remove_alef': True,
            'trim_spaces': True
        }
    },
    
    # Available sheets in folder
    'available_sheets': [],
    
    # Sheet columns (from header row)
    'columns': [],
    
    # Variables
    'variables': [],
    
    # Service accounts
    'accounts': [],
    'accounts_loaded': False,
    
    # Processed names (to detect new ones)
    'processed_names': set(),
}

# Threading control
generator_thread = None
stop_flag = threading.Event()
pause_flag = threading.Event()

SCOPES = ['https://www.googleapis.com/auth/drive', 
          'https://www.googleapis.com/auth/documents',
          'https://www.googleapis.com/auth/presentations',
          'https://www.googleapis.com/auth/spreadsheets']

# ============ HELPERS ============

def extract_id_from_url(url):
    """Extract Google Drive/Docs/Sheets ID from URL"""
    if not url:
        return ''
    
    if '/' not in url and len(url) > 20:
        return url
    
    patterns = [
        r'/d/([a-zA-Z0-9_-]+)',
        r'/folders/([a-zA-Z0-9_-]+)',
        r'/spreadsheets/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return url

def column_to_index(col):
    """Convert column letter to 0-based index (handles upper/lower case)"""
    col = col.strip().upper()
    result = 0
    for char in col:
        if char.isalpha():
            result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1 if result > 0 else 0

def clean_name(name):
    """Clean name by removing titles and prefixes from start AND end"""
    cleanup = state['config'].get('cleanup', {})
    
    if not cleanup.get('enabled', True):
        return name
    
    cleaned = name
    
    # Remove everything before FIRST / ONLY (if it appears near the start)
    # This handles "المهندس / شداد" but keeps "شداد / 1093267308"
    if cleanup.get('remove_before_slash', True) and '/' in cleaned:
        slash_pos = cleaned.index('/')
        # Only remove before slash if it's in first 30% of string (likely a title)
        if slash_pos < len(cleaned) * 0.3:
            cleaned = cleaned.split('/', 1)[-1]  # Split only on first /
    
    # Remove common ID/phone patterns (after main name)
    # Remove "سجل مدني" and anything after it
    cleaned = re.sub(r'سجل مدني.*$', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'رقم.*$', '', cleaned, flags=re.IGNORECASE)
    # Remove standalone numbers (IDs, phones) - 7+ digits
    cleaned = re.sub(r'\s*[/،,]\s*\d{7,}.*$', '', cleaned)
    # Remove trailing / with numbers
    cleaned = re.sub(r'\s*/\s*\d+.*$', '', cleaned)
    
    # Get words to remove
    words = cleanup.get('remove_words', [])
    
    # Sort by length (longest first) to avoid partial matches
    words_sorted = sorted(words, key=len, reverse=True)

    # Build robust patterns to remove titles from start as whole words.
    # We DO NOT remove from end to protect family names like "El Sayed" (السيد).
    # We enforce word boundaries to prevent partial matches (e.g. chopping 'Ahmed').
    if words_sorted:
        words_escaped = [re.escape(w) for w in words_sorted]
        # Match at start, followed by space or end of string.
        begin_pattern = r'^\s*(?:' + '|'.join(words_escaped) + r')(?:[هة])?(?=\s|$)\s*'

        # Repeat removal until no further changes (handles multiple titles)
        prev = None
        while prev != cleaned:
            prev = cleaned
            # Only remove from start
            cleaned = re.sub(begin_pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Remove leading/trailing punctuation (include underscore and dashes)
    cleaned = re.sub(r'^[\s:/،,._\-\u2013\u2014]+', '', cleaned)
    cleaned = re.sub(r'[\s:/،,._\-\u2013\u2014]+$', '', cleaned)
    
    # Remove standalone alef at start (ا or أ possibly followed by punctuation)
    if cleanup.get('remove_alef', True):
        cleaned = re.sub(r'^[اأإ][\s:_\-\u2013\u2014]+', '', cleaned)
    
    # Trim spaces and remove any remaining / at start or end
    if cleanup.get('trim_spaces', True):
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        cleaned = cleaned.strip('/')  # Remove / from start/end
        cleaned = cleaned.strip()  # Final trim

    # Remove single-letter tokens at start or end (e.g., 'د ', 'د.', 'A ')
    if cleanup.get('remove_single_letter_tokens', True):
        # Start: single Arabic/Latin letter optionally followed by a dot and spaces
        cleaned = re.sub(r'^\s*(?:[A-Za-z\u0620-\u06FF])(?:\.)?\s+', '', cleaned)
        # End: single Arabic/Latin letter optionally preceded by spaces and optional dot
        cleaned = re.sub(r'\s+(?:[A-Za-z\u0620-\u06FF])(?:\.)?\s*$', '', cleaned)

    return cleaned

def normalize_name_for_comparison(name):
    """Normalize name for duplicate detection (handle typos)"""
    normalized = name
    
    # Convert to lowercase for comparison
    normalized = normalized.lower()
    
    # Normalize Arabic characters
    normalized = normalized.replace('أ', 'ا')
    normalized = normalized.replace('إ', 'ا')
    normalized = normalized.replace('آ', 'ا')
    normalized = normalized.replace('ى', 'ي')
    normalized = normalized.replace('ة', 'ه')
    
    # Remove all diacritics (tashkeel)
    normalized = re.sub(r'[\u064B-\u065F]', '', normalized)
    
    # Normalize multiple spaces to single space
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

# Lock for thread-safe state updates
state_lock = threading.Lock()

# Lock and runtime state for admin authentication.
auth_lock = threading.Lock()


def _safe_env_int(name, default, minimum):
    """Read integer from env and clamp to a secure minimum."""
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


auth_state = {
    'username': (os.environ.get('ADMIN_USERNAME', 'admin') or 'admin').strip() or 'admin',
    'password_hash': (os.environ.get('ADMIN_PASSWORD_HASH', '') or '').strip(),
    'configured': False,
    'max_attempts': _safe_env_int('LOGIN_MAX_ATTEMPTS', 5, 3),
    'window_seconds': _safe_env_int('LOGIN_WINDOW_SECONDS', 900, 60),
    'lockout_seconds': _safe_env_int('LOGIN_LOCKOUT_SECONDS', 900, 60),
    'single_session_only': os.environ.get('ENFORCE_SINGLE_ADMIN_SESSION', '0') == '1',
    'attempt_buckets': {},
    'active_sessions': set(),
}

_plain_admin_password = (os.environ.get('ADMIN_PASSWORD', '') or '').strip()
if auth_state['password_hash']:
    auth_state['configured'] = True
elif _plain_admin_password:
    auth_state['password_hash'] = generate_password_hash(_plain_admin_password)
    auth_state['configured'] = True
    app_logger.warning('ADMIN_PASSWORD is set as plaintext env. Prefer ADMIN_PASSWORD_HASH.')
else:
    app_logger.warning('Admin login is not configured. Set ADMIN_PASSWORD_HASH to enable login.')

if auth_state['single_session_only']:
    app_logger.info('Admin auth mode: single active session only.')
else:
    app_logger.info('Admin auth mode: concurrent sessions allowed for the same account.')


def _client_ip():
    """Get real client IP when running behind Nginx reverse proxy."""
    forwarded_for = (request.headers.get('X-Forwarded-For', '') or '').strip()
    if forwarded_for:
        return forwarded_for.split(',')[0].strip()
    return request.remote_addr or 'unknown'


def _bucket_key_for_ip(ip):
    return f'ip:{ip}'


def _get_bucket(bucket_key, now_ts):
    bucket = auth_state['attempt_buckets'].get(bucket_key)
    if not bucket:
        bucket = {'count': 0, 'window_start': now_ts, 'locked_until': 0.0}
        auth_state['attempt_buckets'][bucket_key] = bucket
        return bucket

    if now_ts - bucket['window_start'] > auth_state['window_seconds']:
        bucket['count'] = 0
        bucket['window_start'] = now_ts

    return bucket


def _bucket_locked_seconds(bucket, now_ts):
    locked_until = bucket.get('locked_until', 0.0)
    if locked_until <= now_ts:
        return 0
    return int((locked_until - now_ts) + 0.999)


def _get_login_lock_seconds(ip):
    now_ts = time.time()
    with auth_lock:
        ip_bucket = _get_bucket(_bucket_key_for_ip(ip), now_ts)
        global_bucket = _get_bucket('global', now_ts)
        return max(
            _bucket_locked_seconds(ip_bucket, now_ts),
            _bucket_locked_seconds(global_bucket, now_ts)
        )


def _record_failed_login(ip):
    now_ts = time.time()
    with auth_lock:
        for bucket_key in (_bucket_key_for_ip(ip), 'global'):
            bucket = _get_bucket(bucket_key, now_ts)
            bucket['count'] += 1
            if bucket['count'] >= auth_state['max_attempts']:
                bucket['count'] = 0
                bucket['window_start'] = now_ts
                bucket['locked_until'] = now_ts + auth_state['lockout_seconds']


def _clear_login_attempts(ip):
    with auth_lock:
        auth_state['attempt_buckets'].pop(_bucket_key_for_ip(ip), None)
        auth_state['attempt_buckets'].pop('global', None)


def _is_authenticated_session():
    username = (session.get('username') or '').strip()
    session_id = (session.get('session_id') or '').strip()
    if not username or not session_id:
        return False
    if not compare_digest(username, auth_state['username']):
        return False

    with auth_lock:
        active_sessions = auth_state.get('active_sessions', set())

    return session_id in active_sessions


def _get_or_create_csrf_token():
    token = (session.get('csrf_token') or '').strip()
    if token:
        return token
    token = secrets.token_urlsafe(32)
    session['csrf_token'] = token
    return token


def _validate_csrf_token(token):
    expected = (session.get('csrf_token') or '').strip()
    provided = (token or '').strip()
    return bool(expected and provided and compare_digest(expected, provided))


def _sanitize_next_url(next_url):
    candidate = (next_url or '').strip()
    if not candidate or not candidate.startswith('/'):
        return ''
    if candidate.startswith('//'):
        return ''

    # Allow redirect targets only for normal page navigation.
    blocked_exact = {'/logout', '/login'}
    blocked_prefixes = ('/api/', '/socket.io', '/static/')
    if candidate in blocked_exact:
        return ''
    if any(candidate.startswith(prefix) for prefix in blocked_prefixes):
        return ''

    return candidate


def _unauthorized_response():
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Unauthorized'}), 401

    next_url = _sanitize_next_url(request.path)
    if next_url:
        return redirect(url_for('login', next=next_url))
    return redirect(url_for('login'))

def add_log(message, level='info'):
    """Add log message and broadcast"""
    timestamp = time.strftime('%H:%M:%S')
    log_entry = {'time': timestamp, 'message': message, 'level': level}
    
    with state_lock:
        state['logs'].append(log_entry)
        if len(state['logs']) > 500:
            state['logs'] = state['logs'][-500:]
    
    try:
        socketio.emit('log', log_entry, namespace='/')
    except:
        pass

    # Mirror dashboard logs to app.log for server-side troubleshooting.
    try:
        text = f'{timestamp} {message}'
        if level == 'error':
            app_logger.error(text)
        elif level == 'warning':
            app_logger.warning(text)
        else:
            app_logger.info(text)
    except:
        pass

def broadcast_state():
    """Send current state to all clients"""
    try:
        with state_lock:
            data = {
                'status': state['status'],
                'total': state['total'],
                'completed': state['completed'],
                'failed': state['failed'],
                'current_name': state['current_name'],
                'elapsed': time.time() - state['start_time'] if state['start_time'] else 0,
                'rate': state['completed'] / ((time.time() - state['start_time']) / 60) if state['start_time'] and time.time() > state['start_time'] else 0,
            }
        socketio.emit('state_update', data, namespace='/')
    except:
        pass

def load_service_accounts():
    """Load all service account JSON files"""
    state['accounts'] = []

    # Search both legacy root path and structured config directory.
    sa_patterns = ['saedny-*.json', 'service-account-*.json', 'sa-*.json']
    search_dirs = ['.', 'config/service-accounts']
    env_dir = os.environ.get('SERVICE_ACCOUNTS_DIR', '').strip()
    if env_dir:
        search_dirs.insert(0, env_dir)

    sa_files = []

    for base_dir in search_dirs:
        for pattern in sa_patterns:
            sa_files.extend(glob.glob(os.path.join(base_dir, pattern)))
    
    # De-duplicate by canonical path to avoid loading the same file twice
    # when both absolute and relative search dirs point to the same location.
    sa_files = sorted({os.path.realpath(path) for path in sa_files})
    
    if not sa_files:
        add_log('⚠️ No service account files found!', 'error')
        return False
    
    for f in sa_files:
        try:
            creds = service_account.Credentials.from_service_account_file(f, scopes=SCOPES)
            state['accounts'].append({'file': f, 'creds': creds})
            add_log(f'✓ Loaded: {f}', 'success')
        except Exception as e:
            add_log(f'✗ Failed to load {f}: {e}', 'error')
    
    state['accounts_loaded'] = True
    add_log(f'📊 Loaded {len(state["accounts"])} service accounts', 'info')
    return len(state['accounts']) > 0

def get_services(acc_idx=0):
    """Get API services for an account"""
    if not state['accounts']:
        load_service_accounts()
    if not state['accounts']:
        return None, None, None, None
    
    creds = state['accounts'][acc_idx % len(state['accounts'])]['creds']
    return (
        build('drive', 'v3', credentials=creds, cache_discovery=False),
        build('docs', 'v1', credentials=creds, cache_discovery=False),
        build('slides', 'v1', credentials=creds, cache_discovery=False),
        build('sheets', 'v4', credentials=creds, cache_discovery=False)
    )

# ============ FOLDER BROWSING ============

def list_sheets_in_folder(folder_id):
    """List all Google Sheets in a folder"""
    drive, _, _, _ = get_services(0)
    if not drive:
        return []
    
    try:
        results = drive.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
            fields="files(id, name, modifiedTime)",
            orderBy="modifiedTime desc",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        return results.get('files', [])
    except Exception as e:
        add_log(f'❌ Error listing folder: {e}', 'error')
        return []

def list_folder_contents(folder_id):
    """List folders and sheets in a folder"""
    drive, _, _, _ = get_services(0)
    if not drive:
        return {'folders': [], 'sheets': []}
    
    try:
        # Get folders
        folders_result = drive.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            fields="files(id, name)",
            orderBy="name",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        # Get sheets
        sheets_result = drive.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
            fields="files(id, name, modifiedTime)",
            orderBy="modifiedTime desc",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        return {
            'folders': folders_result.get('files', []),
            'sheets': sheets_result.get('files', [])
        }
    except Exception as e:
        add_log(f'❌ Error listing folder: {e}', 'error')
        return {'folders': [], 'sheets': []}

# ============ CERTIFICATE GENERATION ============

class RateLimiter:
    def __init__(self, max_per_min=50):
        self.max = max_per_min
        self.timestamps = []
        self.lock = threading.Lock()
    
    def wait(self):
        with self.lock:
            now = time.time()
            self.timestamps = [t for t in self.timestamps if now - t < 60]
            if len(self.timestamps) >= self.max:
                wait_time = 60 - (now - self.timestamps[0]) + 0.5
                add_log(f'⏳ Rate limit, waiting {wait_time:.0f}s...', 'warning')
                time.sleep(wait_time)
                self.timestamps = []
            self.timestamps.append(time.time())

def process_certificate(acc_idx, row_idx, row_data, rate_limiter):
    """Process one certificate"""
    if stop_flag.is_set():
        return False
    
    while pause_flag.is_set():
        time.sleep(0.5)
        if stop_flag.is_set():
            return False
    
    rate_limiter.wait()
    drive, docs, slides, sheets = get_services(acc_idx)
    
    config = state['config']
    variables = state['variables']
    perf_cfg = config.get('performance', {})
    perf_enabled = perf_cfg.get('enabled', True)
    perf_slow_threshold = float(perf_cfg.get('slow_threshold_sec', 6.0))
    perf_log_each = perf_cfg.get('log_each_certificate', False)
    step_times = {}
    cert_start = time.perf_counter()
    
    # Detect if template is Slides or Docs
    template_type = config.get('template_type', 'doc')  # 'doc' or 'slide'
    
    # Get file name from name column
    name_col = config.get('name_column', '')
    if name_col:
        name_col_idx = column_to_index(name_col)
    else:
        # Fallback to first variable's column or column A
        if variables and variables[0].get('column'):
            name_col_idx = column_to_index(variables[0]['column'])
        else:
            name_col_idx = 0
    
    raw_name = row_data[name_col_idx] if len(row_data) > name_col_idx else f'Certificate_{row_idx}'
    
    # Clean the name
    file_name = clean_name(raw_name)
    
    state['current_name'] = file_name
    broadcast_state()
    
    doc_id = None
    
    # Use shared root folder as temp folder
    temp_folder = config.get('temp_folder_id') or SHARED_ROOT_FOLDER
    
    try:
        # 1. Copy template
        t0 = time.perf_counter()
        doc_id = drive.files().copy(
            fileId=config['template_doc_id'],
            body={'name': file_name, 'parents': [temp_folder]},
            supportsAllDrives=True,
            fields='id'
        ).execute()['id']
        step_times['copy'] = time.perf_counter() - t0
        
        # Small delay helps API consistency without adding much overhead.
        time.sleep(0.1)
        
        # 2. Replace ALL variables
        requests = []
        for idx, var in enumerate(variables):
            placeholder = var['placeholder']
            
            if var['source'] == 'column':
                col_idx = column_to_index(var['column'])
                raw_value = row_data[col_idx] if len(row_data) > col_idx else ''
                # Always clean name values
                value = clean_name(raw_value)
            else:
                value = var.get('value', '')
            
            requests.append({
                'replaceAllText': {
                    'containsText': {'text': placeholder, 'matchCase': True},
                    'replaceText': str(value)
                }
            })
        
        if requests:
            t0 = time.perf_counter()
            if template_type == 'slide':
                # Use Slides API
                slides.presentations().batchUpdate(
                    presentationId=doc_id,
                    body={'requests': requests}
                ).execute()
            else:
                # Use Docs API
                docs.documents().batchUpdate(
                    documentId=doc_id,
                    body={'requests': requests}
                ).execute()
            step_times['replace'] = time.perf_counter() - t0
        else:
            step_times['replace'] = 0.0
        
        # 3. Export as PDF
        t0 = time.perf_counter()
        pdf_data = drive.files().export(fileId=doc_id, mimeType='application/pdf').execute()
        step_times['export'] = time.perf_counter() - t0
        
        # 4. Upload PDF
        t0 = time.perf_counter()
        pdf_file = drive.files().create(
            body={'name': f'{file_name}.pdf', 'parents': [config['target_folder_id']]},
            media_body=MediaIoBaseUpload(io.BytesIO(pdf_data), mimetype='application/pdf'),
            fields='webViewLink',
            supportsAllDrives=True
        ).execute()
        step_times['upload'] = time.perf_counter() - t0
        
        # 5. Delete temp doc (try multiple times)
        t0 = time.perf_counter()
        delete_success = False
        for attempt in range(3):
            try:
                time.sleep(0.1)  # Brief wait so Drive can settle before delete
                drive.files().delete(fileId=doc_id, supportsAllDrives=True).execute()
                delete_success = True
                doc_id = None
                break
            except Exception as del_err:
                if attempt == 2:  # Last attempt
                    # Try to trash instead
                    try:
                        drive.files().update(fileId=doc_id, body={'trashed': True}, supportsAllDrives=True).execute()
                        doc_id = None
                    except:
                        add_log(f'⚠️ Could not delete temp doc: {str(del_err)[:80]}', 'warning')
        step_times['delete'] = time.perf_counter() - t0
        
        # 6. Update sheet with link
        link_col = config['link_column'].upper()
        
        # row_idx is now the actual row number in the sheet
        actual_row = row_idx
        
        t0 = time.perf_counter()
        sheets.spreadsheets().values().update(
            spreadsheetId=config['sheet_id'],
            range=f"{link_col}{actual_row}",
            valueInputOption='RAW',
            body={'values': [[pdf_file['webViewLink']]]}
        ).execute()
        step_times['sheet_update'] = time.perf_counter() - t0
        
        # Track processed names
        with state_lock:
            state['processed_names'].add(file_name)
            state['completed'] += 1
        step_times['total'] = time.perf_counter() - cert_start

        add_log(f'✅ [{state["completed"]}/{state["total"]}] {file_name}', 'success')
        broadcast_state()
        
        return True
        
    except Exception as e:
        step_times['total'] = time.perf_counter() - cert_start
        with state_lock:
            state['failed'] += 1
        add_log(f'❌ {file_name}: {str(e)[:100]}', 'error')
        if doc_id:
            try:
                drive.files().delete(fileId=doc_id, supportsAllDrives=True).execute()
            except:
                pass
        
        broadcast_state()
        return False

def account_worker(acc_idx, items, rate_limiter):
    """Worker for one account"""
    for row_idx, row_data in items:
        if stop_flag.is_set():
            break
        process_certificate(acc_idx, row_idx, row_data, rate_limiter)

def get_pending_rows():
    """Get rows that need certificates"""
    config = state['config']
    
    if not config['sheet_id']:
        return []
    
    try:
        _, _, _, sheets = get_services(0)
        
        # Determine range based on mode
        if config['range_mode'] == 'custom':
            range_start = int(config.get('range_start', 2))
            range_end = int(config.get('range_end', 1000))
            sheet_range = f'A{range_start}:Z{range_end}'
            start_row = range_start
            skip_header = False
        else:
            # All rows mode - start from row 2 (after header)
            sheet_range = 'A2:Z'
            start_row = 2
            skip_header = False
        
        result = sheets.spreadsheets().values().get(
            spreadsheetId=config['sheet_id'],
            range=sheet_range
        ).execute()
        
        rows = result.get('values', [])
        
        link_col_idx = column_to_index(config['link_column'])
        
        # Get name column from first variable
        variables = state.get('variables', [])
        if variables and variables[0].get('column'):
            name_col_idx = column_to_index(variables[0]['column'])
        else:
            name_col_idx = column_to_index('C')  # Default to C
        
        # Check for duplicate names and mark them
        seen_names = {}
        rows_to_mark = {}  # {row_number: first_occurrence_row}
        
        for i, row in enumerate(rows):
            if len(row) > name_col_idx and row[name_col_idx] and row[name_col_idx].strip():
                raw_name = row[name_col_idx].strip()
                cleaned_name = clean_name(raw_name)
                # Normalize for comparison to catch typos like "على" vs "علي"
                normalized_name = normalize_name_for_comparison(cleaned_name)
                actual_row = start_row + i
                
                if normalized_name in seen_names:
                    # Found duplicate - mark it with first occurrence row
                    first_row = seen_names[normalized_name]
                    rows_to_mark[actual_row] = first_row
                    add_log(f'⚠️ Duplicate: "{raw_name}" (cleaned: "{cleaned_name}") at row {actual_row} = row {first_row}', 'warning')
                else:
                    seen_names[normalized_name] = actual_row
        
        # Mark duplicate rows with "مكرر - صف X" in the link column
        if rows_to_mark:
            add_log(f'🔖 Marking {len(rows_to_mark)} duplicate rows...', 'info')
            mark_duplicate_rows(config['sheet_id'], rows_to_mark, config['link_column'], sheets)
            
            # Re-read the sheet after marking duplicates
            result = sheets.spreadsheets().values().get(
                spreadsheetId=config['sheet_id'],
                range=sheet_range
            ).execute()
            rows = result.get('values', [])
        
        todo = []
        for i, row in enumerate(rows):
            has_link = len(row) > link_col_idx and row[link_col_idx] and row[link_col_idx].strip()
            # Check if marked as duplicate (starts with "مكرر")
            is_duplicate = has_link and row[link_col_idx].strip().startswith('مكرر')
            has_name = len(row) > name_col_idx and row[name_col_idx] and row[name_col_idx].strip()
            
            # Only add to todo if has name and no link (or link is http - real link)
            if has_name and not has_link:
                # Store actual row number in sheet (1-based)
                actual_row = start_row + i
                todo.append((actual_row, row))
            elif has_name and is_duplicate:
                # Skip duplicates silently (already logged above)
                pass
        
        return todo
    except Exception as e:
        add_log(f'❌ Error reading sheet: {e}', 'error')
        return []

def mark_duplicate_rows(sheet_id, row_mapping, link_column, sheets):
    """Mark duplicate rows with 'مكرر - صف X' in the link column"""
    try:
        # Prepare batch update
        # row_mapping is {duplicate_row: first_occurrence_row}
        data = []
        for dup_row, first_row in row_mapping.items():
            data.append({
                'range': f'{link_column}{dup_row}',
                'values': [[f'مكرر - صف {first_row}']]
            })
        
        if data:
            sheets.spreadsheets().values().batchUpdate(
                spreadsheetId=sheet_id,
                body={
                    'valueInputOption': 'RAW',
                    'data': data
                }
            ).execute()
            add_log(f'✅ Marked {len(row_mapping)} duplicate rows', 'success')
    except Exception as e:
        add_log(f'⚠️ Could not mark duplicate rows: {e}', 'warning')

def retry_failed_certificates():
    """Retry generation for certificates that don't have links"""
    if state['retry_count'] >= state['max_retries']:
        add_log(f'⚠️ Reached maximum retry attempts ({state["max_retries"]})', 'warning')
        return False
    
    state['retry_count'] += 1
    add_log(f'🔄 Starting retry attempt {state["retry_count"]}/{state["max_retries"]}...', 'info')
    
    # Get pending rows (those without links)
    todo = get_pending_rows()
    
    if not todo:
        add_log('✅ No pending certificates to retry', 'success')
        return False
    
    add_log(f'📝 Found {len(todo)} certificates to retry', 'info')
    
    # Run generator with the pending items
    run_generator(todo, is_retry=True)
    
    return True

def run_generator(todo=None, is_retry=False):
    """Main generator function"""
    global state
    
    stop_flag.clear()
    pause_flag.clear()
    
    state['status'] = 'running'
    state['start_time'] = time.time()
    
    broadcast_state()
    add_log('🚀 Starting certificate generation...', 'info')
    
    if not state['accounts_loaded']:
        if not load_service_accounts():
            state['status'] = 'idle'
            add_log('❌ Cannot start: No service accounts!', 'error')
            broadcast_state()
            return
    
    config = state['config']
    
    # Only require template, target folder, and sheet (temp folder uses shared root)
    if not all([config['template_doc_id'], config['target_folder_id'], config['sheet_id']]):
        state['status'] = 'idle'
        add_log('❌ Missing configuration! Please fill all required fields.', 'error')
        broadcast_state()
        return
    
    try:
        if todo is None:
            add_log('📖 Reading spreadsheet...', 'info')
            todo = get_pending_rows()
        
        state['total'] = len(todo)
        state['completed'] = 0
        state['failed'] = 0
        
        add_log(f'📝 {len(todo)} certificates to generate', 'info')
        broadcast_state()
        
        if not todo:
            state['status'] = 'idle'
            add_log('✅ No pending certificates.', 'success')
            broadcast_state()
            return
        
        # Distribute among accounts
        num_accounts = len(state['accounts'])
        batches = [[] for _ in range(num_accounts)]
        
        for idx, item in enumerate(todo):
            batches[idx % num_accounts].append(item)
        
        rate_limiters = [RateLimiter(50) for _ in range(num_accounts)]
        
        threads = []
        for acc_idx, batch in enumerate(batches):
            if batch:
                add_log(f'👤 Account {acc_idx}: {len(batch)} items', 'info')
                t = threading.Thread(target=account_worker, args=(acc_idx, batch, rate_limiters[acc_idx]))
                t.start()
                threads.append(t)
        
        for t in threads:
            t.join()

        if stop_flag.is_set():
            state['status'] = 'stopped'
            add_log('⏹️ Generation stopped by user', 'warning')
            broadcast_state()
            return
        
        elapsed = time.time() - state['start_time']
        rate = state['completed'] / (elapsed / 60) if elapsed > 0 else 0
        
        add_log(f'🎉 Batch completed! {state["completed"]} certificates in {elapsed/60:.1f} minutes ({rate:.0f}/min)', 'success')
        
        # Check if there are failed certificates and retry
        if state['failed'] > 0 and not is_retry:
            add_log(f'⚠️ {state["failed"]} certificates failed. Preparing to retry...', 'warning')
            time.sleep(2)  # Wait 2 seconds before retry
            
            # Reset retry counter for new batch
            state['retry_count'] = 0
            
            # Try to retry failed certificates
            retry_success = retry_failed_certificates()
            
            # If retry was performed, the status will be set by the retry run
            if not retry_success:
                state['status'] = 'completed'
        else:
            state['status'] = 'completed'
            if is_retry:
                add_log(f'✅ Retry completed. Total completed: {state["completed"]}, Failed: {state["failed"]}', 'success')
        
        broadcast_state()
        
    except Exception as e:
        state['status'] = 'idle'
        add_log(f'💥 Error: {str(e)}', 'error')
        broadcast_state()

# ============ DRIVE BROWSER ============

# Shared Drive folder ID (the root folder shared with all service accounts)
SHARED_ROOT_FOLDER = '0AHlyd4Og76tkUk9PVA'

def list_drive_files(folder_id='root', file_type='all'):
    """List files in a Google Drive folder"""
    drive, _, _, _ = get_services(0)
    if not drive:
        return []
    
    try:
        # Use shared root folder if 'root' is requested
        if folder_id == 'root':
            folder_id = SHARED_ROOT_FOLDER
        
        parent_query = f"'{folder_id}' in parents"
        
        # Determine what to show based on type
        if file_type == 'folder':
            # Show only folders
            mime_query = "mimeType='application/vnd.google-apps.folder'"
        elif file_type == 'doc':
            # Show folders, Google Docs, and Google Slides
            mime_query = "(mimeType='application/vnd.google-apps.folder' or mimeType='application/vnd.google-apps.document' or mimeType='application/vnd.google-apps.presentation')"
        elif file_type == 'sheet':
            # Show folders and Google Sheets
            mime_query = "(mimeType='application/vnd.google-apps.folder' or mimeType='application/vnd.google-apps.spreadsheet')"
        else:
            # Show all
            mime_query = "mimeType != 'application/vnd.google-apps.form'"
        
        query = f"{parent_query} and {mime_query} and trashed=false"
        
        results = drive.files().list(
            q=query,
            fields="files(id, name, mimeType)",
            orderBy="folder,name",
            pageSize=100,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = []
        for f in results.get('files', []):
            files.append({
                'id': f['id'],
                'name': f['name'],
                'mimeType': f['mimeType'],
                'isFolder': f['mimeType'] == 'application/vnd.google-apps.folder'
            })
        
        # Sort: folders first, then by name
        files.sort(key=lambda x: (not x['isFolder'], x['name'].lower()))
        
        return files
        
    except Exception as e:
        add_log(f'❌ Error listing drive: {e}', 'error')
        return []

def get_sheet_columns(sheet_id):
    """Get column headers from first row of sheet"""
    try:
        _, _, _, sheets = get_services(0)
        result = sheets.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='1:1'  # First row only
        ).execute()
        
        headers = result.get('values', [[]])[0]
        columns = []
        for idx, header in enumerate(headers):
            col_letter = ''
            n = idx
            while n >= 0:
                col_letter = chr(n % 26 + ord('A')) + col_letter
                n = n // 26 - 1
            columns.append({
                'letter': col_letter,
                'name': header.strip() if header else f'Column {col_letter}',
                'index': idx
            })
        add_log(f'📊 Loaded {len(columns)} columns from sheet', 'info')
        return columns
    except Exception as e:
        import traceback
        print(f"Error reading sheet columns: {e}")
        print(traceback.format_exc())
        add_log(f'⚠️ Could not read sheet headers: {str(e)[:100]}', 'warning')
        return []

def detect_single_variable_in_background(template_id, template_type, name_col):
    """Detect first template variable asynchronously to keep /api/config fast."""
    # If name column is not ready yet, wait briefly for sheet metadata thread.
    wait_started = time.time()
    while (not name_col or str(name_col).strip().upper() == 'A') and (time.time() - wait_started) < 8:
        time.sleep(0.5)
        with state_lock:
            if state['config'].get('template_doc_id') != template_id:
                return
            latest_name_col = state['config'].get('name_column', '')
        if latest_name_col:
            name_col = latest_name_col

    detected = detect_template_variables(template_id, template_type)
    if not detected:
        return

    with state_lock:
        # Ignore stale result if user changed template while detection was running.
        if state['config'].get('template_doc_id') != template_id:
            return

        first_var = detected[0]
        state['variables'] = [{
            'placeholder': first_var,
            'source': 'column',
            'column': name_col if name_col else 'A',
            'description': 'الاسم'
        }]

    add_log(f'🔍 Detected variable: {first_var} → Column {name_col or "A"}', 'info')
    broadcast_state()

def refresh_sheet_metadata_in_background(sheet_id):
    """Load sheet columns and link/name columns asynchronously."""
    started_at = time.time()
    try:
        add_log('⏳ Background: loading sheet metadata...', 'info')
        columns = get_sheet_columns(sheet_id)
        link_column = find_or_create_link_column(sheet_id, columns)
        name_column = auto_detect_name_column(columns)

        with state_lock:
            # Ignore stale results if user changed sheet while task was running.
            if state['config'].get('sheet_id') != sheet_id:
                return
            state['columns'] = columns
            state['config']['link_column'] = link_column or state['config'].get('link_column', 'O') or 'O'
            state['config']['name_column'] = name_column or state['config'].get('name_column', '')

        elapsed = time.time() - started_at
        add_log(f'✅ Background: sheet metadata ready in {elapsed:.2f}s', 'success')
        broadcast_state()
    except Exception as e:
        add_log(f'⚠️ Background: sheet metadata failed: {str(e)[:100]}', 'warning')
        broadcast_state()

def auto_detect_name_column(columns):
    """Auto-detect column with name based on header"""
    if not columns:
        return ''
    
    # Search for column with name-like headers
    name_keywords = ['اسم', 'الاسم', 'name', 'الإسم', 'أسم', 'اﻻسم']
    
    for col in columns:
        header_lower = col['name'].lower().strip()
        for keyword in name_keywords:
            if keyword in header_lower:
                add_log(f'✅ Auto-detected name column: {col["letter"]} ({col["name"]})', 'success')
                return col['letter']
    
    # Fallback to first column
    if columns:
        add_log(f'⚠️ No name column found, using first column: {columns[0]["letter"]}', 'warning')
        return columns[0]['letter']
    
    return 'A'

def find_or_create_link_column(sheet_id, columns=None):
    """Find 'رابط الشهادة' column or create it as next header column."""
    try:
        started_at = time.time()
        _, _, _, sheets = get_services(0)

        # Reuse already-loaded header row when available to avoid an extra read.
        if columns is None:
            result = sheets.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range='1:1'
            ).execute()
            headers = result.get('values', [[]])[0]
            header_columns = []
            for idx, header in enumerate(headers):
                col_letter = ''
                n = idx
                while n >= 0:
                    col_letter = chr(n % 26 + ord('A')) + col_letter
                    n = n // 26 - 1
                header_columns.append({
                    'letter': col_letter,
                    'name': header.strip() if header else '',
                    'index': idx
                })
        else:
            header_columns = columns
            headers = [c.get('name', '') for c in columns]
        
        # Search for existing "رابط الشهادة" column
        link_column_names = ['رابط الشهادة', 'رابط الشهاده', 'Certificate Link', 'certificate_link', 'Link']
        for col in header_columns:
            idx = col.get('index', 0)
            header = col.get('name', '')
            header_clean = header.strip().lower() if header else ''
            for name in link_column_names:
                if name.lower() in header_clean or header_clean in name.lower():
                    col_letter = col.get('letter')
                    if not col_letter:
                        col_letter = ''
                        n = idx
                        while n >= 0:
                            col_letter = chr(n % 26 + ord('A')) + col_letter
                            n = n // 26 - 1
                    add_log(f'🔗 Found link column "{header}" at {col_letter}', 'info')
                    elapsed = time.time() - started_at
                    add_log(f'⏱️ Link column check completed in {elapsed:.2f}s', 'info')
                    return col_letter

        # Not found - append after current header width.
        next_col_idx = len(headers)
        col_letter = ''
        n = next_col_idx
        while n >= 0:
            col_letter = chr(n % 26 + ord('A')) + col_letter
            n = n // 26 - 1

        # Write header with name "رابط الشهادة"
        sheets.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f'{col_letter}1',
            valueInputOption='RAW',
            body={'values': [['رابط الشهادة']]}
        ).execute()

        add_log(f'🔗 Created link column "رابط الشهادة" at {col_letter}', 'success')
        elapsed = time.time() - started_at
        add_log(f'⏱️ Link column check completed in {elapsed:.2f}s', 'info')
        return col_letter
        
    except Exception as e:
        import traceback
        print(f"Error in find_or_create_link_column: {e}")
        print(traceback.format_exc())
        add_log(f'⚠️ Could not find/create link column: {str(e)[:100]}', 'warning')
        return 'O'  # Default fallback

# ============ ROUTES ============


@app.before_request
def enforce_authentication():
    endpoint = request.endpoint or ''

    # Login page and static assets must remain public.
    if endpoint in ('login', 'static'):
        return None

    # Socket.IO performs its own authentication check in connect handler.
    if request.path.startswith('/socket.io'):
        return None

    # Fail closed when admin credentials are not configured yet.
    if not auth_state['configured']:
        return redirect(url_for('login'))

    if not _is_authenticated_session():
        session.clear()
        return _unauthorized_response()

    session.permanent = True
    return None


@app.after_request
def add_response_security_headers(response):
    if request.endpoint in ('login', 'logout'):
        response.headers['Cache-Control'] = 'no-store'
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('X-Frame-Options', 'SAMEORIGIN')
    return response


@app.route('/login', methods=['GET', 'POST'])
def login():
    if auth_state['configured'] and _is_authenticated_session():
        return redirect(url_for('index'))

    error_message = ''
    lock_seconds = 0
    next_url = _sanitize_next_url(request.values.get('next', '')) or url_for('index')
    username_value = auth_state['username']

    if request.method == 'POST':
        username_value = (request.form.get('username', '') or '').strip()
        client_ip = _client_ip()
        lock_seconds = _get_login_lock_seconds(client_ip)

        if lock_seconds > 0:
            error_message = f'تم قفل تسجيل الدخول مؤقتًا. حاول بعد {lock_seconds} ثانية.'
        elif not auth_state['configured']:
            error_message = 'الحساب غير مُهيأ بعد. عيّن ADMIN_PASSWORD_HASH ثم أعد تشغيل الخدمة.'
        elif not _validate_csrf_token(request.form.get('csrf_token', '')):
            error_message = 'انتهت صلاحية جلسة تسجيل الدخول. أعد المحاولة.'
        else:
            password = request.form.get('password', '') or ''
            username_ok = compare_digest(username_value, auth_state['username'])
            password_ok = False
            if username_ok:
                try:
                    password_ok = check_password_hash(auth_state['password_hash'], password)
                except Exception:
                    password_ok = False

            if username_ok and password_ok:
                session_id = secrets.token_urlsafe(32)
                with auth_lock:
                    if auth_state['single_session_only']:
                        auth_state['active_sessions'] = {session_id}
                    else:
                        auth_state['active_sessions'].add(session_id)

                _clear_login_attempts(client_ip)
                session.clear()
                session.permanent = True
                session['username'] = auth_state['username']
                session['session_id'] = session_id
                session['csrf_token'] = secrets.token_urlsafe(32)
                add_log('🔐 Admin logged in', 'info')
                return redirect(next_url)

            _record_failed_login(client_ip)
            time.sleep(0.35)
            lock_seconds = _get_login_lock_seconds(client_ip)
            if lock_seconds > 0:
                error_message = f'محاولات كثيرة. تم القفل لمدة {lock_seconds} ثانية.'
            else:
                error_message = 'اسم المستخدم أو كلمة المرور غير صحيحة.'
            app_logger.warning('Failed admin login attempt from %s', client_ip)

    csrf_token = _get_or_create_csrf_token()
    return render_template(
        'login.html',
        csrf_token=csrf_token,
        next_url=next_url,
        error_message=error_message,
        lock_seconds=lock_seconds,
        auth_configured=auth_state['configured'],
        single_session_only=auth_state['single_session_only'],
        username_value=username_value,
    )


@app.route('/logout', methods=['POST'])
def logout():
    if not _validate_csrf_token(request.form.get('csrf_token', '')):
        return redirect(url_for('index'))

    current_session_id = (session.get('session_id') or '').strip()
    with auth_lock:
        if current_session_id:
            auth_state['active_sessions'].discard(current_session_id)

    session.clear()
    add_log('🔓 Admin logged out', 'info')
    return redirect(url_for('login'))

@app.route('/')
def index():
    return render_template(
        'index.html',
        csrf_token=_get_or_create_csrf_token(),
        admin_username=auth_state['username']
    )

@app.route('/api/drive/list')
def api_drive_list():
    """API to list files in Google Drive"""
    folder_id = request.args.get('folder_id', 'root')
    file_type = request.args.get('type', 'all')
    
    files = list_drive_files(folder_id, file_type)
    return jsonify({'files': files})

@app.route('/api/sheet/columns')
def api_sheet_columns():
    """Get column headers from sheet"""
    sheet_id = request.args.get('sheet_id', '')
    if not sheet_id:
        return jsonify({'columns': []})
    
    columns = get_sheet_columns(sheet_id)
    return jsonify({'columns': columns})

@app.route('/api/state')
def get_state():
    return jsonify({
        'status': state['status'],
        'total': state['total'],
        'completed': state['completed'],
        'failed': state['failed'],
        'retry_count': state.get('retry_count', 0),
        'max_retries': state.get('max_retries', 2),
        'config': state['config'],
        'variables': state['variables'],
        'columns': state.get('columns', []),
        'accounts_count': len(state['accounts']),
        'available_sheets': state['available_sheets'],
        'logs': state['logs'][-50:]
    })

@app.route('/api/config', methods=['POST'])
def save_config():
    data = request.json

    prev_template_id = state['config'].get('template_doc_id', '')
    prev_sheet_id = state['config'].get('sheet_id', '')
    
    # Template - now receives ID directly
    state['config']['template_doc_id'] = data.get('template_doc_id', '')
    state['config']['template_doc_name'] = data.get('template_doc_name', '')

    template_changed = state['config']['template_doc_id'] != prev_template_id
    
    # Prefer template_type from frontend selection to avoid extra Drive request.
    template_type_input = (data.get('template_type') or '').strip().lower()
    if template_type_input in ('doc', 'slide'):
        state['config']['template_type'] = template_type_input
    elif state['config']['template_doc_id'] and (template_changed or not state['config'].get('template_type')):
        try:
            drive, _, _, _ = get_services(0)
            file_info = drive.files().get(
                fileId=state['config']['template_doc_id'],
                fields='mimeType',
                supportsAllDrives=True
            ).execute()
            mime_type = file_info.get('mimeType', '')

            if 'presentation' in mime_type:
                state['config']['template_type'] = 'slide'
                add_log('📊 Detected template type: Google Slides', 'info')
            elif 'document' in mime_type:
                state['config']['template_type'] = 'doc'
                add_log('📄 Detected template type: Google Docs', 'info')
            else:
                state['config']['template_type'] = 'doc'
                add_log(f'⚠️ Unknown template type: {mime_type}, defaulting to Docs', 'warning')
        except Exception as e:
            state['config']['template_type'] = 'doc'
            add_log(f'⚠️ Could not detect template type: {str(e)[:50]}', 'warning')
    else:
        state['config']['template_type'] = state['config'].get('template_type', 'doc') or 'doc'
    
    # Target folder
    state['config']['target_folder_id'] = data.get('target_folder_id', '')
    state['config']['target_folder_name'] = data.get('target_folder_name', '')
    
    # Temp folder
    state['config']['temp_folder_id'] = data.get('temp_folder_id', '')
    state['config']['temp_folder_name'] = data.get('temp_folder_name', '')
    
    # Sheet
    state['config']['sheet_id'] = data.get('sheet_id', '')
    state['config']['sheet_name'] = data.get('sheet_name', '')

    sheet_changed = state['config']['sheet_id'] != prev_sheet_id
    
    # Load sheet metadata in background to keep config save responsive.
    if state['config']['sheet_id']:
        if sheet_changed or not state.get('columns'):
            state['columns'] = []
            add_log('⏳ Scheduling sheet metadata refresh in background...', 'info')
            threading.Thread(
                target=refresh_sheet_metadata_in_background,
                args=(state['config']['sheet_id'],),
                daemon=True
            ).start()
    else:
        state['columns'] = []
        state['config']['link_column'] = 'O'
        state['config']['name_column'] = ''
    
    state['config']['range_mode'] = data.get('range_mode', 'all')
    state['config']['range_start'] = int(data.get('range_start', 2))
    state['config']['range_end'] = int(data.get('range_end', 1000))
    # Auto-watch is deprecated and intentionally disabled.
    state['config']['auto_watch'] = False
    state['config']['watch_interval'] = 30
    
    add_log('⚙️ Configuration saved', 'info')

    # Detect first variable in background so save responds quickly.
    if state['config']['template_doc_id']:
        if template_changed or not state.get('variables'):
            template_type = state['config'].get('template_type', 'doc')
            name_col = state['config'].get('name_column', '')
            add_log('⏳ Detecting template variables in background...', 'info')
            threading.Thread(
                target=detect_single_variable_in_background,
                args=(state['config']['template_doc_id'], template_type, name_col),
                daemon=True
            ).start()
    else:
        state['variables'] = []
    
    return jsonify({'success': True, 'config': state['config'], 'variables': state['variables'], 'columns': state['columns']})

def detect_template_variables(template_id, template_type='doc'):
    """Detect {{VARIABLE}} patterns in template document or presentation"""
    try:
        drive, docs, slides, _ = get_services(0)
        
        full_text = ''
        
        if template_type == 'slide':
            # Get Slides presentation
            presentation = slides.presentations().get(presentationId=template_id).execute()
            
            # Extract text from all slides
            for slide in presentation.get('slides', []):
                for element in slide.get('pageElements', []):
                    if 'shape' in element:
                        shape = element['shape']
                        if 'text' in shape:
                            for text_elem in shape['text'].get('textElements', []):
                                if 'textRun' in text_elem:
                                    full_text += text_elem['textRun'].get('content', '')
        else:
            # Get Docs document
            doc = docs.documents().get(documentId=template_id).execute()
            
            # Extract all text from document
            content = doc.get('body', {}).get('content', [])
            
            for element in content:
                if 'paragraph' in element:
                    for elem in element['paragraph'].get('elements', []):
                        if 'textRun' in elem:
                            full_text += elem['textRun'].get('content', '')
        
        # Find all <<VARIABLE>> patterns (Arabic and English)
        import re
        # Match <<text>> where text can be Arabic, English, numbers, spaces, or underscores
        variables = re.findall(r'<<([\u0600-\u06FFa-zA-Z0-9_\s]+)>>', full_text)
        
        # Return unique variables with <<>> format
        unique_vars = list(dict.fromkeys(['<<' + v.strip() + '>>' for v in variables]))
        return unique_vars
        
    except Exception as e:
        add_log(f'⚠️ Could not read template: {str(e)[:50]}', 'warning')
        return []

@app.route('/api/detect-variables', methods=['POST'])
def api_detect_variables():
    """Manually trigger variable detection"""
    data = request.json or {}
    
    # Get template URL from request or from config
    template_url = data.get('template_url', '')
    if template_url:
        template_id = extract_id_from_url(template_url)
    else:
        template_id = state['config'].get('template_doc_id', '')
    
    if not template_id:
        return jsonify({'error': 'No template configured'}), 400
    
    template_type = (data.get('template_type') or state['config'].get('template_type') or 'doc').strip().lower()
    if template_type not in ('doc', 'slide'):
        template_type = 'doc'

    detected = detect_template_variables(template_id, template_type)
    if detected:
        # Update variables
        existing_placeholders = {v['placeholder']: v for v in state['variables']}
        new_variables = []
        for placeholder in detected:
            if placeholder in existing_placeholders:
                new_variables.append(existing_placeholders[placeholder])
            else:
                new_variables.append({
                    'placeholder': placeholder,
                    'source': 'column',
                    'column': '',
                    'description': ''
                })
        state['variables'] = new_variables
        add_log(f'🔍 Detected {len(detected)} variables', 'success')
        # Return detected as list of strings (placeholders only)
        return jsonify({'success': True, 'variables': detected})
    
    return jsonify({'success': False, 'error': 'No variables found'})

@app.route('/api/variables', methods=['POST'])
def save_variables():
    state['variables'] = request.json.get('variables', [])
    add_log(f'📝 Saved {len(state["variables"])} variables', 'info')
    return jsonify({'success': True})

@app.route('/api/cleanup-config', methods=['POST'])
def save_cleanup_config():
    data = request.json
    state['config']['cleanup'] = {
        'enabled': data.get('enabled', True),
        'remove_words': data.get('remove_words', []),
        'remove_before_slash': data.get('remove_before_slash', True),
        'remove_alef': data.get('remove_alef', True),
        'trim_spaces': data.get('trim_spaces', True)
    }
    add_log(f'🧹 Saved cleanup config ({len(state["config"]["cleanup"]["remove_words"])} words)', 'info')
    return jsonify({'success': True})

@app.route('/api/start', methods=['POST'])
def start_generation():
    global generator_thread
    
    if state['status'] == 'running':
        return jsonify({'error': 'Already running'}), 400
    
    # Reset retry counter when starting fresh
    state['retry_count'] = 0
    
    generator_thread = threading.Thread(target=run_generator)
    generator_thread.start()
    
    return jsonify({'success': True})

@app.route('/api/pause', methods=['POST'])
def pause_generation():
    if pause_flag.is_set():
        pause_flag.clear()
        state['status'] = 'running'
        add_log('▶️ Resumed', 'info')
    else:
        pause_flag.set()
        state['status'] = 'paused'
        add_log('⏸️ Paused', 'warning')
    
    broadcast_state()
    return jsonify({'success': True, 'paused': pause_flag.is_set()})

@app.route('/api/stop', methods=['POST'])
def stop_generation():
    stop_flag.set()
    pause_flag.clear()
    state['status'] = 'stopped'
    add_log('⏹️ Stopped', 'warning')
    broadcast_state()
    return jsonify({'success': True})

@app.route('/api/auto-watch', methods=['POST'])
def toggle_auto_watch():
    state['config']['auto_watch'] = False
    add_log('ℹ️ Auto-watch is disabled in this version', 'info')
    return jsonify({'success': False, 'message': 'Auto-watch is disabled', 'watching': False}), 410

@app.route('/api/reload-accounts', methods=['POST'])
def reload_accounts():
    state['accounts'] = []
    state['accounts_loaded'] = False
    load_service_accounts()
    return jsonify({'success': True, 'count': len(state['accounts'])})

# ============ SOCKETIO ============

@socketio.on('connect')
def handle_connect():
    if not auth_state['configured'] or not _is_authenticated_session():
        return False

    emit('state_update', {
        'status': state['status'],
        'total': state['total'],
        'completed': state['completed'],
        'failed': state['failed'],
        'current_name': state['current_name'],
    })

# ============ MAIN ============

if __name__ == '__main__':
    import sys
    
    host = os.environ.get('APP_HOST', '127.0.0.1')
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    
    print("\n" + "="*50)
    print("  📜 Certificate Generator Dashboard v2")
    print("="*50)
    print(f"\n  Bind address: {host}:{port}")
    if host in ('127.0.0.1', 'localhost'):
        print("  Access externally through Nginx on port 80/443")
    else:
        print(f"  Open in browser: http://{host}:{port}")
    print("")
    
    load_service_accounts()
    
    # Production mode (no debug)
    socketio.run(app, host=host, port=port, debug=False, allow_unsafe_werkzeug=True)
