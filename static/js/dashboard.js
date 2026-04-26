const socket = io();

let sheetColumns = [];
let currentBrowserTarget = null;
let currentBrowserType = null;
let currentFolderId = 'root';
let folderHistory = [{ id: 'root', name: 'الملفات المشتركة' }];
let currentBrowserFiles = [];
let currentBrowserQuery = '';
let isSavingConfig = false;
let isStoppingGeneration = false;
let pendingSaveLogResolver = null;
const CONFIG_SAVE_SLOW_NOTICE_MS = 25000;
const CONFIG_SAVE_REQUEST_TIMEOUT_MS = 5000;
const CONFIG_SAVE_EARLY_CONFIRM_TIMEOUT_MS = 45000;
const CONFIG_SAVE_CONFIRM_TIMEOUT_MS = 120000;
const CONFIG_SAVE_CONFIRM_INTERVAL_MS = 3000;
const CONFIG_STATE_POLL_TIMEOUT_MS = 4000;

const nativeFetch = window.fetch.bind(window);
window.fetch = async (...args) => {
    const response = await nativeFetch(...args);
    if (response.status === 401) {
        window.location.href = '/login';
        throw new Error('Unauthorized');
    }
    return response;
};

document.addEventListener('DOMContentLoaded', () => {
    bindModalShortcuts();
    loadState();
});

socket.on('connect', () => console.log('Socket connected'));
socket.on('state_update', (data) => updateUI(data));
socket.on('log', (log) => {
    addLog(log);

    if (isSavingConfig && pendingSaveLogResolver && isConfigSavedLog(log)) {
        pendingSaveLogResolver({ kind: 'log_saved' });
        pendingSaveLogResolver = null;
    }
});

function showTab(tabId, tabButton) {
    document.querySelectorAll('.tab').forEach((tab) => tab.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach((content) => content.classList.remove('active'));

    if (tabButton) {
        tabButton.classList.add('active');
    }

    const tabContent = document.getElementById('tab-' + tabId);
    if (tabContent) {
        tabContent.classList.add('active');
    }
}

function toggleRangeMode() {
    const modeInput = document.querySelector('input[name="rangeMode"]:checked');
    const mode = modeInput ? modeInput.value : 'all';
    document.getElementById('customRangeInputs').style.display = mode === 'custom' ? 'grid' : 'none';
}

function bindModalShortcuts() {
    const browserModal = document.getElementById('fileBrowserModal');
    const stopModal = document.getElementById('stopConfirmModal');

    browserModal.addEventListener('click', (event) => {
        if (event.target.id === 'fileBrowserModal') {
            closeBrowser();
        }
    });

    stopModal.addEventListener('click', (event) => {
        if (event.target.id === 'stopConfirmModal') {
            closeStopConfirmModal();
        }
    });

    document.addEventListener('keydown', (event) => {
        if (event.key !== 'Escape') {
            return;
        }

        if (stopModal.classList.contains('active')) {
            closeStopConfirmModal();
            return;
        }

        if (browserModal.classList.contains('active')) {
            closeBrowser();
        }
    });
}

function openStopConfirmModal() {
    document.getElementById('stopConfirmModal').classList.add('active');
}

function closeStopConfirmModal() {
    document.getElementById('stopConfirmModal').classList.remove('active');
}

function showToast(message, type = 'info') {
    const stack = document.getElementById('toastStack');
    const toast = document.createElement('div');
    toast.className = 'toast ' + type;
    toast.textContent = message;
    stack.appendChild(toast);

    setTimeout(() => {
        toast.remove();
    }, 3200);
}

function isConfigSavedLog(log) {
    const message = String(log && log.message ? log.message : '');
    return message.includes('Configuration saved') || message.includes('تم حفظ الإعدادات');
}

async function reloadAccounts() {
    try {
        const res = await fetch('/api/reload-accounts', { method: 'POST' });
        const data = await res.json();

        if (!res.ok) {
            showToast('تعذر تحديث الحسابات', 'error');
            return;
        }

        showToast('تم تحديث الحسابات: ' + (data.count || 0), 'success');
        await loadState();
    } catch (error) {
        showToast('تعذر الاتصال بالخادم', 'error');
    }
}

async function loadSheetColumns(sheetId) {
    if (!sheetId) {
        sheetColumns = [];
        return;
    }

    try {
        const res = await fetch('/api/sheet/columns?sheet_id=' + sheetId);
        const data = await res.json();
        sheetColumns = data.columns || [];
    } catch (error) {
        console.error('Error loading columns', error);
        sheetColumns = [];
    }
}

function openBrowser(target, type) {
    currentBrowserTarget = target;
    currentBrowserType = type;
    currentFolderId = 'root';
    folderHistory = [{ id: 'root', name: 'الملفات المشتركة' }];
    currentBrowserFiles = [];
    currentBrowserQuery = '';

    const titles = {
        doc: 'اختيار ملف القالب',
        folder: 'اختيار مجلد',
        sheet: 'اختيار جدول البيانات'
    };

    document.getElementById('browserTitle').textContent = titles[type] || 'تصفح الملفات';
    document.getElementById('fileBrowserModal').classList.add('active');

    const searchInput = document.getElementById('browserSearchInput');
    const searchCount = document.getElementById('browserSearchCount');
    if (searchInput) {
        searchInput.value = '';
        searchInput.focus();
    }
    if (searchCount) {
        searchCount.textContent = '';
    }

    updateBreadcrumb();
    loadFiles('root');
}

function closeBrowser() {
    document.getElementById('fileBrowserModal').classList.remove('active');
}

function updateBreadcrumb() {
    const breadcrumb = document.getElementById('breadcrumb');
    breadcrumb.innerHTML = '';

    folderHistory.forEach((folder, index) => {
        const button = document.createElement('button');
        button.textContent = folder.name;
        button.onclick = () => navigateTo(folder.id, index);
        breadcrumb.appendChild(button);
    });
}

function navigateTo(folderId, index) {
    if (typeof index === 'number') {
        folderHistory = folderHistory.slice(0, index + 1);
    }

    currentFolderId = folderId;
    updateBreadcrumb();
    loadFiles(folderId);
}

function normalizeArabicText(text) {
    return String(text || '')
        .toLowerCase()
        .replace(/[\u064B-\u065F\u0670]/g, '')
        .replace(/[\u0610-\u061A]/g, '')
        .replace(/[أإآٱ]/g, 'ا')
        .replace(/ة/g, 'ه')
        .replace(/ى/g, 'ي')
        .replace(/[ؤئ]/g, 'ء')
        .replace(/ـ/g, '')
        .replace(/[\u200c\u200d]/g, '')
        .replace(/[^\p{L}\p{N}\s]/gu, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function splitTokens(text) {
    const normalized = normalizeArabicText(text);
    return normalized ? normalized.split(' ').filter(Boolean) : [];
}

function isSubsequence(needle, haystack) {
    if (!needle) {
        return true;
    }
    let i = 0;
    let j = 0;
    while (i < needle.length && j < haystack.length) {
        if (needle[i] === haystack[j]) {
            i += 1;
        }
        j += 1;
    }
    return i === needle.length;
}

function levenshteinDistanceWithLimit(a, b, limit) {
    if (Math.abs(a.length - b.length) > limit) {
        return limit + 1;
    }

    const prev = new Array(b.length + 1);
    const curr = new Array(b.length + 1);

    for (let j = 0; j <= b.length; j += 1) {
        prev[j] = j;
    }

    for (let i = 1; i <= a.length; i += 1) {
        curr[0] = i;
        let rowMin = curr[0];

        for (let j = 1; j <= b.length; j += 1) {
            const cost = a[i - 1] === b[j - 1] ? 0 : 1;
            curr[j] = Math.min(
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + cost
            );
            if (curr[j] < rowMin) {
                rowMin = curr[j];
            }
        }

        if (rowMin > limit) {
            return limit + 1;
        }

        for (let j = 0; j <= b.length; j += 1) {
            prev[j] = curr[j];
        }
    }

    return prev[b.length];
}

function fuzzyTokenMatch(queryToken, candidateText, candidateTokens) {
    if (!queryToken) {
        return true;
    }

    if (candidateText.includes(queryToken)) {
        return true;
    }

    if (isSubsequence(queryToken, candidateText)) {
        return true;
    }

    const limit = queryToken.length <= 4 ? 1 : 2;
    for (const token of candidateTokens) {
        if (!token) {
            continue;
        }
        if (token.includes(queryToken) || queryToken.includes(token)) {
            return true;
        }
        if (isSubsequence(queryToken, token)) {
            return true;
        }
        if (levenshteinDistanceWithLimit(queryToken, token, limit) <= limit) {
            return true;
        }
    }

    return false;
}

function filterBrowserFiles(files, query) {
    const queryTokens = splitTokens(query);
    if (!queryTokens.length) {
        return files;
    }

    return files.filter((file) => {
        const meta = fileTypeMeta(file);
        const candidateText = normalizeArabicText(`${file.name} ${meta.label} ${meta.tag}`);
        const candidateTokens = candidateText ? candidateText.split(' ') : [];

        return queryTokens.every((token) => fuzzyTokenMatch(token, candidateText, candidateTokens));
    });
}

function updateBrowserSearchCount(total, filtered) {
    const searchCount = document.getElementById('browserSearchCount');
    if (!searchCount) {
        return;
    }

    if (!currentBrowserQuery.trim()) {
        searchCount.textContent = `عدد العناصر: ${total}`;
        return;
    }

    searchCount.textContent = `نتائج البحث: ${filtered} من ${total}`;
}

function renderBrowserFiles(files) {
    const fileList = document.getElementById('fileList');
    fileList.innerHTML = '';

    const filteredFiles = filterBrowserFiles(files, currentBrowserQuery);
    updateBrowserSearchCount(files.length, filteredFiles.length);

    if (!filteredFiles.length) {
        const empty = document.createElement('li');
        empty.className = 'empty';
        empty.textContent = currentBrowserQuery
            ? 'لا توجد نتائج مطابقة. جرّب كتابة مختلفة أو أقصر.'
            : 'لا توجد ملفات في هذا المسار';
        fileList.appendChild(empty);
        return;
    }

    filteredFiles.forEach((file) => {
        const item = document.createElement('li');
        item.className = 'file-item' + (file.isFolder ? ' folder' : '');

        const fileMeta = fileTypeMeta(file);

        const icon = document.createElement('i');
        icon.className = 'bi ' + fileMeta.icon + ' file-icon';
        icon.setAttribute('aria-hidden', 'true');

        const type = document.createElement('span');
        type.className = 'file-type';
        type.textContent = fileMeta.label;

        const name = document.createElement('div');
        name.className = 'file-name';
        name.textContent = file.name;

        item.appendChild(icon);
        item.appendChild(type);
        item.appendChild(name);

        if (currentBrowserType === 'folder' && file.isFolder) {
            const actionWrap = document.createElement('div');
            actionWrap.className = 'file-actions';

            const selectBtn = document.createElement('button');
            selectBtn.textContent = 'اختيار';
            selectBtn.onclick = (event) => {
                event.stopPropagation();
                selectFile(file);
            };

            actionWrap.appendChild(selectBtn);
            item.appendChild(actionWrap);
        }

        item.onclick = () => {
            if (file.isFolder) {
                folderHistory.push({ id: file.id, name: file.name });
                navigateTo(file.id);
                return;
            }
            selectFile(file);
        };

        fileList.appendChild(item);
    });
}

function handleBrowserSearchInput() {
    const input = document.getElementById('browserSearchInput');
    currentBrowserQuery = input ? input.value : '';
    renderBrowserFiles(currentBrowserFiles);
}

function fileTypeMeta(file) {
    if (file.isFolder) {
        return {
            label: 'مجلد',
            tag: 'مجلد',
            icon: 'bi-folder2'
        };
    }

    const mime = file.mimeType || '';
    if (mime.includes('spreadsheet')) {
        return {
            label: 'جدول بيانات',
            tag: 'جدول',
            icon: 'bi-file-earmark-spreadsheet'
        };
    }

    if (mime.includes('presentation')) {
        return {
            label: 'عرض تقديمي',
            tag: 'عرض',
            icon: 'bi-file-earmark-slides'
        };
    }

    if (mime.includes('document')) {
        return {
            label: 'مستند',
            tag: 'مستند',
            icon: 'bi-file-earmark-richtext'
        };
    }

    if (mime.includes('pdf')) {
        return {
            label: 'ملف PDF',
            tag: 'PDF',
            icon: 'bi-filetype-pdf'
        };
    }

    return {
        label: 'ملف',
        tag: 'ملف',
        icon: 'bi-file-earmark'
    };
}

async function loadFiles(folderId) {
    const loading = document.getElementById('loadingFiles');
    const fileList = document.getElementById('fileList');

    loading.style.display = 'block';
    fileList.innerHTML = '';

    try {
        const res = await fetch('/api/drive/list?folder_id=' + folderId + '&type=' + currentBrowserType);
        const data = await res.json();

        loading.style.display = 'none';
        currentBrowserFiles = Array.isArray(data.files) ? data.files : [];
        renderBrowserFiles(currentBrowserFiles);
    } catch (error) {
        loading.style.display = 'none';
        currentBrowserFiles = [];
        updateBrowserSearchCount(0, 0);
        fileList.innerHTML = '<li class="error">فشل تحميل الملفات</li>';
    }
}

function showSelection(target, file, tag, iconClass = '') {
    document.getElementById(target + 'Url').value = file.id;
    document.getElementById(target + 'Name').textContent = file.name;

    const selected = document.getElementById(target + 'Selected');
    selected.querySelector('.file-tag').textContent = tag;

    const icon = selected.querySelector('.file-icon');
    if (icon) {
        const resolvedIcon = iconClass || fileTypeMeta(file).icon;
        icon.className = 'bi ' + resolvedIcon + ' file-icon';
    }

    selected.classList.remove('is-hidden');
}

function selectFile(file) {
    const target = currentBrowserTarget;
    const fileMeta = fileTypeMeta(file);

    if (target === 'template') {
        const mimeType = (file && file.mimeType) ? file.mimeType : '';
        const templateType = mimeType.includes('presentation') ? 'slide' : 'doc';
        document.getElementById('templateType').value = templateType;
        showSelection('template', file, fileMeta.tag, fileMeta.icon);
    } else if (target === 'targetFolder') {
        showSelection('targetFolder', file, 'مجلد', 'bi-folder2');
    } else if (target === 'sheet') {
        showSelection('sheet', file, 'جدول', 'bi-file-earmark-spreadsheet');
        loadSheetColumns(file.id);
    }

    closeBrowser();
}

function clearSelection(target) {
    document.getElementById(target + 'Url').value = '';
    const selected = document.getElementById(target + 'Selected');
    selected.classList.add('is-hidden');
    document.getElementById(target + 'Name').textContent = '';

    if (target === 'template') {
        document.getElementById('templateType').value = 'doc';
    }

    if (target === 'sheet') {
        sheetColumns = [];
    }
}

async function loadState() {
    try {
        const res = await fetch('/api/state');
        const data = await res.json();

        updateUI(data);

        if (data.config) {
            document.getElementById('templateUrl').value = data.config.template_doc_id || '';
            document.getElementById('templateType').value = data.config.template_type || 'doc';
            document.getElementById('targetFolderUrl').value = data.config.target_folder_id || '';
            document.getElementById('tempFolderUrl').value = data.config.temp_folder_id || '';
            document.getElementById('sheetUrl').value = data.config.sheet_id || '';

            if (data.config.template_doc_name) {
                showSelection('template', {
                    id: data.config.template_doc_id,
                    name: data.config.template_doc_name
                }, 'مستند', 'bi-file-earmark-richtext');
            }

            if (data.config.target_folder_name) {
                showSelection('targetFolder', {
                    id: data.config.target_folder_id,
                    name: data.config.target_folder_name
                }, 'مجلد', 'bi-folder2');
            }

            if (data.config.sheet_name) {
                showSelection('sheet', {
                    id: data.config.sheet_id,
                    name: data.config.sheet_name
                }, 'جدول', 'bi-file-earmark-spreadsheet');
            }

            const rangeMode = data.config.range_mode || 'all';
            const rangeModeInput = document.querySelector('input[name="rangeMode"][value="' + rangeMode + '"]');
            if (rangeModeInput) {
                rangeModeInput.checked = true;
            }

            document.getElementById('rangeStart').value = data.config.range_start || 2;
            document.getElementById('rangeEnd').value = data.config.range_end || 1000;
            toggleRangeMode();

            if (data.config.cleanup) {
                document.getElementById('cleanupEnabled').checked = data.config.cleanup.enabled !== false;
                document.getElementById('removeWords').value = (data.config.cleanup.remove_words || []).join('\n');
                document.getElementById('removeBeforeSlash').checked = data.config.cleanup.remove_before_slash !== false;
                document.getElementById('removeAlef').checked = data.config.cleanup.remove_alef !== false;
                document.getElementById('trimSpaces').checked = data.config.cleanup.trim_spaces !== false;
            }
        }

        if (data.columns && data.columns.length > 0) {
            sheetColumns = data.columns;
        }

        if (data.config && data.config.link_column) {
            document.getElementById('linkColumn').value = data.config.link_column;
        }

        if (data.config && data.config.name_column) {
            document.getElementById('nameColumn').value = data.config.name_column;
        }

        if (data.logs) {
            const logsContainer = document.getElementById('logsContainer');
            logsContainer.innerHTML = '';
            data.logs.forEach((log) => addLog(log, false));
        }
    } catch (error) {
        console.error(error);
        showToast('تعذر تحميل الحالة الحالية', 'error');
    }
}

function normalizeStatus(status) {
    if (status === 'watching') {
        return 'idle';
    }
    return status || 'idle';
}

function updateUI(data) {
    document.getElementById('totalCount').textContent = data.total || 0;
    document.getElementById('completedCount').textContent = data.completed || 0;
    document.getElementById('failedCount').textContent = data.failed || 0;
    document.getElementById('rateCount').textContent = Math.round(data.rate || 0);

    const total = data.total || 0;
    const completed = data.completed || 0;
    const progress = total > 0 ? Math.round((completed / total) * 100) : 0;
    const progressBar = document.getElementById('progressBar');
    progressBar.style.width = progress + '%';
    progressBar.textContent = progress + '%';

    const status = normalizeStatus(data.status);

    if (data.current_name && status === 'running') {
        document.getElementById('currentName').textContent = data.current_name;
    } else if (status === 'stopped') {
        document.getElementById('currentName').textContent = 'تم إيقاف العملية يدويا';
    } else if (status === 'idle' || status === 'completed') {
        document.getElementById('currentName').textContent = 'في انتظار بدء المعالجة';
    }

    const labels = {
        idle: 'جاهز',
        running: 'قيد التشغيل',
        paused: 'متوقف مؤقتا',
        stopped: 'تم الإيقاف',
        completed: 'مكتمل'
    };

    const indicator = document.getElementById('statusIndicator');
    indicator.className = 'status-indicator status-' + status;
    document.getElementById('statusText').textContent = labels[status] || 'جاهز';

    const isRunning = status === 'running' || status === 'paused';
    const isStopped = status === 'idle' || status === 'completed' || status === 'stopped';

    document.getElementById('btnStart').disabled = isRunning;
    document.getElementById('btnStop').disabled = isStopped;
}

function sanitizeLogMessage(message) {
    const text = String(message || '');
    try {
        return text.replace(/\p{Extended_Pictographic}/gu, '').trim();
    } catch (_) {
        return text;
    }
}

function addLog(log, scroll = true) {
    const container = document.getElementById('logsContainer');
    const entry = document.createElement('div');
    entry.className = 'log-entry ' + (log.level || 'info');

    const time = document.createElement('span');
    time.className = 'log-time';
    time.textContent = log.time || '--:--:--';

    const message = document.createElement('span');
    message.className = 'log-message';
    message.textContent = sanitizeLogMessage(log.message);

    entry.appendChild(time);
    entry.appendChild(message);
    container.appendChild(entry);

    if (scroll) {
        container.scrollTop = container.scrollHeight;
    }
}

function clearLogsView() {
    document.getElementById('logsContainer').innerHTML = '';
    showToast('تم تنظيف عرض السجل', 'info');
}

async function parseJsonSafe(response) {
    const responseText = await response.text();
    if (!responseText) {
        return {};
    }

    try {
        return JSON.parse(responseText);
    } catch (error) {
        return {};
    }
}

function waitMs(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function applyConfigSaveResult(data) {
    if (data.columns && data.columns.length > 0) {
        sheetColumns = data.columns;
    }

    if (data.config && data.config.link_column) {
        document.getElementById('linkColumn').value = data.config.link_column;
    }

    if (data.config && data.config.name_column) {
        document.getElementById('nameColumn').value = data.config.name_column;
    }
}

async function finalizeConfigSaveSuccess(data, toastMessage) {
    applyConfigSaveResult(data || {});

    try {
        await loadState();
    } catch (_) {
        // Ignore reload issues here; success already confirmed.
    }

    showToast(toastMessage || 'تم حفظ الإعدادات بنجاح', 'success');
}

function isSameSavedConfig(serverConfig, requestedConfig) {
    if (!serverConfig || !requestedConfig) {
        return false;
    }

    return (
        (serverConfig.template_doc_id || '') === (requestedConfig.template_doc_id || '') &&
        (serverConfig.target_folder_id || '') === (requestedConfig.target_folder_id || '') &&
        (serverConfig.sheet_id || '') === (requestedConfig.sheet_id || '') &&
        (serverConfig.range_mode || 'all') === (requestedConfig.range_mode || 'all') &&
        parseInt(serverConfig.range_start || 2, 10) === parseInt(requestedConfig.range_start || 2, 10) &&
        parseInt(serverConfig.range_end || 1000, 10) === parseInt(requestedConfig.range_end || 1000, 10)
    );
}

async function confirmConfigSaved(requestedConfig, timeoutMs = CONFIG_SAVE_CONFIRM_TIMEOUT_MS) {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() <= deadline) {
        let pollTimeoutId = null;
        const pollController = new AbortController();
        try {
            pollTimeoutId = setTimeout(() => pollController.abort(), CONFIG_STATE_POLL_TIMEOUT_MS);
            const stateRes = await fetch('/api/state', {
                cache: 'no-store',
                signal: pollController.signal
            });
            if (stateRes.ok) {
                const stateData = await parseJsonSafe(stateRes);
                const serverConfig = stateData.config || {};
                if (isSameSavedConfig(serverConfig, requestedConfig)) {
                    return { saved: true, stateData };
                }
            }
        } catch (error) {
            // Ignore transient connectivity issues during confirmation polling.
        } finally {
            if (pollTimeoutId) {
                clearTimeout(pollTimeoutId);
            }
        }

        await waitMs(CONFIG_SAVE_CONFIRM_INTERVAL_MS);
    }

    return { saved: false, stateData: null };
}

async function requestConfigSave(config, controller) {
    try {
        const res = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config),
            signal: controller.signal
        });

        const data = await parseJsonSafe(res);
        return {
            kind: 'fetch',
            ok: res.ok,
            status: res.status,
            data
        };
    } catch (error) {
        return {
            kind: 'fetch_error',
            error
        };
    }
}

async function saveConfig() {
    const saveButton = document.getElementById('saveConfigBtn');
    const saveButtonText = saveButton ? saveButton.querySelector('span') : null;
    const originalButtonLabel = saveButtonText ? saveButtonText.textContent : '';
    let slowNoticeId = null;
    let requestTimeoutId = null;

    try {
        if (isSavingConfig) {
            showToast('يوجد حفظ قيد التنفيذ بالفعل', 'info');
            return;
        }
        isSavingConfig = true;

        if (saveButton) {
            saveButton.disabled = true;
        }
        if (saveButtonText) {
            saveButtonText.textContent = 'جاري الحفظ...';
        }

        const selectedRange = document.querySelector('input[name="rangeMode"]:checked');
        if (!selectedRange) {
            showToast('تعذر تحديد نطاق الصفوف', 'error');
            return;
        }

        const config = {
            template_doc_id: document.getElementById('templateUrl').value,
            template_doc_name: document.getElementById('templateName').textContent || '',
            template_type: document.getElementById('templateType').value || 'doc',
            target_folder_id: document.getElementById('targetFolderUrl').value,
            target_folder_name: document.getElementById('targetFolderName').textContent || '',
            sheet_id: document.getElementById('sheetUrl').value,
            sheet_name: document.getElementById('sheetName').textContent || '',
            range_mode: selectedRange.value,
            range_start: parseInt(document.getElementById('rangeStart').value, 10) || 2,
            range_end: parseInt(document.getElementById('rangeEnd').value, 10) || 1000
        };

        if (!config.template_doc_id || !config.target_folder_id || !config.sheet_id) {
            showToast('اكمل اختيار القالب والمجلد والشيت قبل الحفظ', 'warning');
            return;
        }

        showToast('جاري حفظ الإعدادات والتحقق من Google... قد يستغرق ذلك قليلا', 'info');

        slowNoticeId = setTimeout(() => {
            showToast('ما زال الحفظ جاريا... انتظر حتى اكتمال التحقق من القالب والشيت', 'info');
        }, CONFIG_SAVE_SLOW_NOTICE_MS);

        const controller = new AbortController();
        requestTimeoutId = setTimeout(() => controller.abort(), CONFIG_SAVE_REQUEST_TIMEOUT_MS);

        const fetchOutcomePromise = requestConfigSave(config, controller);
        const earlyConfirmPromise = confirmConfigSaved(config, CONFIG_SAVE_EARLY_CONFIRM_TIMEOUT_MS)
            .then((result) => ({ kind: 'confirm', result }));
        const logOutcomePromise = new Promise((resolve) => {
            pendingSaveLogResolver = resolve;
        });

        let firstOutcome = await Promise.race([fetchOutcomePromise, earlyConfirmPromise, logOutcomePromise]);

        if (firstOutcome.kind === 'log_saved') {
            const confirmed = await confirmConfigSaved(config, 15000);
            if (confirmed.saved) {
                if (!controller.signal.aborted) {
                    controller.abort();
                }
                await finalizeConfigSaveSuccess({
                    config: confirmed.stateData?.config || {},
                    columns: confirmed.stateData?.columns || []
                }, 'تم حفظ الإعدادات بنجاح على الخادم');
                return;
            }

            firstOutcome = await fetchOutcomePromise;
        }

        if (firstOutcome.kind === 'confirm' && firstOutcome.result.saved) {
            if (!controller.signal.aborted) {
                controller.abort();
            }
            await finalizeConfigSaveSuccess({
                config: firstOutcome.result.stateData?.config || {},
                columns: firstOutcome.result.stateData?.columns || []
            }, 'تم حفظ الإعدادات بنجاح على الخادم');
            return;
        }

        if (firstOutcome.kind === 'confirm' && !firstOutcome.result.saved) {
            firstOutcome = await fetchOutcomePromise;
        }

        if (firstOutcome.kind === 'fetch_error') {
            if (firstOutcome.error && firstOutcome.error.name === 'AbortError') {
                const confirmed = await confirmConfigSaved(config);
                if (confirmed.saved) {
                    await finalizeConfigSaveSuccess({
                        config: confirmed.stateData?.config || {},
                        columns: confirmed.stateData?.columns || []
                    }, 'تم حفظ الإعدادات بنجاح على الخادم');
                    return;
                }
                showToast('تعذر تأكيد الحفظ بعد انتهاء المهلة. حاول مرة أخرى.', 'error');
                return;
            }

            throw firstOutcome.error;
        }

        const data = firstOutcome.data || {};
        if (!firstOutcome.ok) {
            const confirmed = await confirmConfigSaved(config, 20000);
            if (confirmed.saved) {
                await finalizeConfigSaveSuccess({
                    config: confirmed.stateData?.config || {},
                    columns: confirmed.stateData?.columns || []
                }, 'تم حفظ الإعدادات بنجاح على الخادم');
                return;
            }
            showToast(data.error || 'تعذر حفظ الإعدادات', 'error');
            return;
        }

        await finalizeConfigSaveSuccess(data, 'تم حفظ الإعدادات بنجاح');
    } catch (error) {
        showToast('تعذر الاتصال بالخادم', 'error');
    } finally {
        pendingSaveLogResolver = null;
        if (slowNoticeId) {
            clearTimeout(slowNoticeId);
        }
        if (requestTimeoutId) {
            clearTimeout(requestTimeoutId);
        }
        isSavingConfig = false;
        if (saveButton) {
            saveButton.disabled = false;
        }
        if (saveButtonText) {
            saveButtonText.textContent = originalButtonLabel || 'حفظ الإعدادات';
        }
    }
}

async function startGeneration() {
    document.getElementById('btnStart').disabled = true;
    document.getElementById('currentName').textContent = 'جاري بدء المعالجة...';

    try {
        const res = await fetch('/api/start', { method: 'POST' });
        const data = await res.json();

        if (!res.ok) {
            showToast('تعذر البدء: ' + (data.error || 'خطأ غير معروف'), 'error');
            document.getElementById('btnStart').disabled = false;
            return;
        }

        showToast('تم بدء الإصدار', 'success');
    } catch (error) {
        document.getElementById('btnStart').disabled = false;
        showToast('تعذر الاتصال بالخادم', 'error');
    }
}

async function stopGeneration() {
    const statusText = document.getElementById('statusText').textContent || '';
    if (statusText !== 'قيد التشغيل' && statusText !== 'متوقف مؤقتا') {
        showToast('لا توجد عملية قيد التشغيل لإيقافها', 'info');
        return;
    }

    openStopConfirmModal();
}

async function confirmStopGeneration() {
    if (isStoppingGeneration) {
        return;
    }

    isStoppingGeneration = true;
    const confirmButton = document.getElementById('confirmStopBtn');
    const originalText = confirmButton ? confirmButton.textContent : '';

    if (confirmButton) {
        confirmButton.disabled = true;
        confirmButton.textContent = 'جاري الإيقاف...';
    }

    closeStopConfirmModal();

    try {
        const res = await fetch('/api/stop', { method: 'POST' });
        if (!res.ok) {
            showToast('تعذر إيقاف العملية', 'error');
            return;
        }

        await loadState();

        showToast('تم إيقاف العملية', 'warning');
    } catch (error) {
        showToast('تعذر الاتصال بالخادم', 'error');
    } finally {
        isStoppingGeneration = false;
        if (confirmButton) {
            confirmButton.disabled = false;
            confirmButton.textContent = originalText || 'نعم، إيقاف الآن';
        }
    }
}

async function saveCleanupConfig() {
    const cleanupConfig = {
        enabled: document.getElementById('cleanupEnabled').checked,
        remove_words: document.getElementById('removeWords').value
            .split('\n')
            .map((word) => word.trim())
            .filter(Boolean),
        remove_before_slash: document.getElementById('removeBeforeSlash').checked,
        remove_alef: document.getElementById('removeAlef').checked,
        trim_spaces: document.getElementById('trimSpaces').checked
    };

    try {
        const res = await fetch('/api/cleanup-config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(cleanupConfig)
        });

        if (!res.ok) {
            showToast('تعذر حفظ إعدادات التنظيف', 'error');
            return;
        }

        showToast('تم حفظ إعدادات التنظيف', 'success');
    } catch (error) {
        showToast('تعذر الاتصال بالخادم', 'error');
    }
}
