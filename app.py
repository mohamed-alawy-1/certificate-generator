#!/usr/bin/env python3
"""
Certificate Generator Dashboard v2
- Browse Google Drive folders
- Select sheets from folder
- Auto-trigger when new names added
- Multiple variables support
"""

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import threading
import time
import re
import json
import os
import glob
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

app = Flask(__name__)
app.config['SECRET_KEY'] = 'certificate-generator-secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ============ STATE ============
state = {
    'status': 'idle',  # idle, running, paused, completed, watching
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
watcher_thread = None
stop_flag = threading.Event()
pause_flag = threading.Event()
watch_stop_flag = threading.Event()

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
                'watching': state['config']['auto_watch'],
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
    
    sa_files = sorted(set(sa_files))
    
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

        if perf_enabled and (perf_log_each or step_times['total'] >= perf_slow_threshold):
            perf_level = 'warning' if step_times['total'] >= perf_slow_threshold else 'info'
            add_log(
                (
                    f'⏱️ Perf {file_name}: copy={step_times.get("copy", 0):.2f}s '
                    f'replace={step_times.get("replace", 0):.2f}s '
                    f'export={step_times.get("export", 0):.2f}s '
                    f'upload={step_times.get("upload", 0):.2f}s '
                    f'delete={step_times.get("delete", 0):.2f}s '
                    f'sheet={step_times.get("sheet_update", 0):.2f}s '
                    f'total={step_times.get("total", 0):.2f}s '
                    f'[acc={acc_idx}]'
                ),
                perf_level
            )
        
        add_log(f'✅ [{state["completed"]}/{state["total"]}] {file_name}', 'success')
        broadcast_state()
        
        return True
        
    except Exception as e:
        step_times['total'] = time.perf_counter() - cert_start
        with state_lock:
            state['failed'] += 1
        add_log(f'❌ {file_name}: {str(e)[:100]}', 'error')
        if perf_enabled:
            add_log(
                (
                    f'⏱️ Perf failed {file_name}: copy={step_times.get("copy", 0):.2f}s '
                    f'replace={step_times.get("replace", 0):.2f}s '
                    f'export={step_times.get("export", 0):.2f}s '
                    f'upload={step_times.get("upload", 0):.2f}s '
                    f'delete={step_times.get("delete", 0):.2f}s '
                    f'sheet={step_times.get("sheet_update", 0):.2f}s '
                    f'total={step_times.get("total", 0):.2f}s '
                    f'[acc={acc_idx}]'
                ),
                'warning'
            )
        
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
            state['status'] = 'watching' if config['auto_watch'] else 'idle'
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
                state['status'] = 'watching' if config['auto_watch'] else 'completed'
        else:
            state['status'] = 'watching' if config['auto_watch'] else 'completed'
            if is_retry:
                add_log(f'✅ Retry completed. Total completed: {state["completed"]}, Failed: {state["failed"]}', 'success')
        
        broadcast_state()
        
    except Exception as e:
        state['status'] = 'idle'
        add_log(f'💥 Error: {str(e)}', 'error')
        broadcast_state()

# ============ AUTO WATCHER ============

def watcher_loop():
    """Watch for new entries and auto-generate"""
    add_log('👁️ Auto-watch started. Checking every {}s...'.format(state['config']['watch_interval']), 'info')
    
    while not watch_stop_flag.is_set():
        if state['status'] not in ['running', 'paused']:
            # Check for new entries
            todo = get_pending_rows()
            
            if todo:
                add_log(f'🆕 Found {len(todo)} new entries!', 'info')
                state['status'] = 'running'
                run_generator(todo)
            
        # Wait for interval
        for _ in range(state['config']['watch_interval']):
            if watch_stop_flag.is_set():
                break
            time.sleep(1)
    
    add_log('👁️ Auto-watch stopped', 'warning')

def start_watcher():
    global watcher_thread
    
    if watcher_thread and watcher_thread.is_alive():
        return
    
    watch_stop_flag.clear()
    watcher_thread = threading.Thread(target=watcher_loop, daemon=True)
    watcher_thread.start()
    state['config']['auto_watch'] = True
    state['status'] = 'watching'
    broadcast_state()

def stop_watcher():
    watch_stop_flag.set()
    state['config']['auto_watch'] = False
    if state['status'] == 'watching':
        state['status'] = 'idle'
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

def find_or_create_link_column(sheet_id):
    """Find 'رابط الشهادة' column or create it as last column"""
    try:
        _, _, _, sheets = get_services(0)
        
        # Get all rows to find actual last column with data
        result = sheets.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='1:100'  # Check first 100 rows
        ).execute()
        
        rows = result.get('values', [])
        if not rows:
            # Empty sheet - start at column A
            sheets.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range='A1',
                valueInputOption='RAW',
                body={'values': [['رابط الشهادة']]}
            ).execute()
            add_log('🔗 Created link column "رابط الشهادة" at A', 'success')
            return 'A'
        
        headers = rows[0] if rows else []
        
        # Search for existing "رابط الشهادة" column
        link_column_names = ['رابط الشهادة', 'رابط الشهاده', 'Certificate Link', 'certificate_link', 'Link']
        for idx, header in enumerate(headers):
            header_clean = header.strip().lower() if header else ''
            for name in link_column_names:
                if name.lower() in header_clean or header_clean in name.lower():
                    # Found! Calculate column letter
                    col_letter = ''
                    n = idx
                    while n >= 0:
                        col_letter = chr(n % 26 + ord('A')) + col_letter
                        n = n // 26 - 1
                    add_log(f'🔗 Found link column "{header}" at {col_letter}', 'info')
                    return col_letter
        
        # Not found - find the last non-empty column across all rows
        max_col = 0
        for row in rows:
            # Find last non-empty cell in this row
            for i in range(len(row) - 1, -1, -1):
                if row[i] and str(row[i]).strip():
                    max_col = max(max_col, i)
                    break
        
        # Add column after last used column
        next_col_idx = max_col + 1
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
        return col_letter
        
    except Exception as e:
        import traceback
        print(f"Error in find_or_create_link_column: {e}")
        print(traceback.format_exc())
        add_log(f'⚠️ Could not find/create link column: {str(e)[:100]}', 'warning')
        return 'O'  # Default fallback

# ============ ROUTES ============

@app.route('/')
def index():
    return render_template('index.html')

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
    
    # Template - now receives ID directly
    state['config']['template_doc_id'] = data.get('template_doc_id', '')
    state['config']['template_doc_name'] = data.get('template_doc_name', '')
    
    # Auto-detect template type from Drive API
    if state['config']['template_doc_id']:
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
                state['config']['template_type'] = 'doc'  # Default
                add_log(f'⚠️ Unknown template type: {mime_type}, defaulting to Docs', 'warning')
        except Exception as e:
            state['config']['template_type'] = 'doc'  # Default on error
            add_log(f'⚠️ Could not detect template type: {str(e)[:50]}', 'warning')
    else:
        state['config']['template_type'] = 'doc'
    
    # Target folder
    state['config']['target_folder_id'] = data.get('target_folder_id', '')
    state['config']['target_folder_name'] = data.get('target_folder_name', '')
    
    # Temp folder
    state['config']['temp_folder_id'] = data.get('temp_folder_id', '')
    state['config']['temp_folder_name'] = data.get('temp_folder_name', '')
    
    # Sheet
    state['config']['sheet_id'] = data.get('sheet_id', '')
    state['config']['sheet_name'] = data.get('sheet_name', '')
    
    # Load columns if sheet changed
    if state['config']['sheet_id']:
        state['columns'] = get_sheet_columns(state['config']['sheet_id'])
        # Auto-detect or create link column
        state['config']['link_column'] = find_or_create_link_column(state['config']['sheet_id'])
        # Auto-detect name column
        state['config']['name_column'] = auto_detect_name_column(state['columns'])
    else:
        state['columns'] = []
        state['config']['link_column'] = 'O'
        state['config']['name_column'] = ''
    
    state['config']['range_mode'] = data.get('range_mode', 'all')
    state['config']['range_start'] = int(data.get('range_start', 2))
    state['config']['range_end'] = int(data.get('range_end', 1000))
    state['config']['watch_interval'] = int(data.get('watch_interval', 30))
    
    add_log('⚙️ Configuration saved', 'info')
    
    # Auto-detect single variable from template
    if state['config']['template_doc_id']:
        template_type = state['config'].get('template_type', 'doc')
        detected = detect_template_variables(state['config']['template_doc_id'], template_type)
        if detected:
            # Use first detected variable as the name variable
            first_var = detected[0]
            name_col = state['config'].get('name_column', '')
            state['variables'] = [{
                'placeholder': first_var,
                'source': 'column',
                'column': name_col if name_col else 'A',
                'description': 'الاسم'
            }]
            add_log(f'🔍 Detected variable: {first_var} → Column {name_col or "A"}', 'info')
    
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
    
    detected = detect_template_variables(template_id)
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
    stop_watcher()
    state['status'] = 'idle'
    add_log('⏹️ Stopped', 'warning')
    broadcast_state()
    return jsonify({'success': True})

@app.route('/api/auto-watch', methods=['POST'])
def toggle_auto_watch():
    data = request.json
    enable = data.get('enable', False)
    
    if enable:
        start_watcher()
        add_log('👁️ Auto-watch enabled', 'success')
    else:
        stop_watcher()
        add_log('👁️ Auto-watch disabled', 'warning')
    
    return jsonify({'success': True, 'watching': state['config']['auto_watch']})

@app.route('/api/reload-accounts', methods=['POST'])
def reload_accounts():
    state['accounts'] = []
    state['accounts_loaded'] = False
    load_service_accounts()
    return jsonify({'success': True, 'count': len(state['accounts'])})

# ============ SOCKETIO ============

@socketio.on('connect')
def handle_connect():
    emit('state_update', {
        'status': state['status'],
        'total': state['total'],
        'completed': state['completed'],
        'failed': state['failed'],
        'current_name': state['current_name'],
        'watching': state['config']['auto_watch'],
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
