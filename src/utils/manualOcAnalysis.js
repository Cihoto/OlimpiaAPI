const BATCH_REASON_BY_STATUS = Object.freeze({
    ready: 'Analizado correctamente',
    duplicate_batch: 'OC duplicada en la misma tanda',
    duplicate_backend: 'OC ya procesada en backend',
    conflict: 'Misma OC con datos distintos entre archivos',
    missing_oc: 'No se detecto numero de OC',
    error: 'No se pudo analizar el archivo',
    address_not_found: 'Direccion no encontrada en base de clientes'
});

function normalizeText(value) {
    return String(value || '')
        .toLowerCase()
        .normalize('NFD')
        .replace(/\p{Diacritic}/gu, '')
        .trim();
}

function isAddressNotFoundError({ normalizedError, attemptedAddresses }) {
    if (attemptedAddresses.length > 0) {
        return true;
    }
    if (!normalizedError) {
        return false;
    }
    const hasDireccion = normalizedError.includes('direccion');
    const hasClientes = normalizedError.includes('base de clientes');
    const hasNoMatch = normalizedError.includes('no se encontro')
        || normalizedError.includes('no se encontro coincidencia')
        || normalizedError.includes('coincidencia de direccion');
    return hasDireccion && (hasNoMatch || hasClientes);
}

function canManualOcBatchEntryBeSubmitted(status) {
    return String(status || '').trim() === 'ready';
}

function buildManualOcSuccessAnalysis({
    code = 'ok',
    reason = 'Analizado correctamente'
} = {}) {
    return {
        analyzed: true,
        ok: true,
        canSubmitToInvoicer: true,
        code,
        reason
    };
}

function classifyManualOcParserFailure(parser = {}) {
    const parserStatusRaw = Number(parser?.status);
    const parserStatus = Number.isFinite(parserStatusRaw) ? parserStatusRaw : null;
    const parserBody = parser?.body && typeof parser.body === 'object' ? parser.body : {};
    const parserErrorText = String(
        parserBody?.error
        || parserBody?.message
        || parserBody?.details
        || ''
    ).trim();
    const attemptedAddresses = Array.isArray(parserBody?.direccionesProbadas)
        ? parserBody.direccionesProbadas
            .map((address) => String(address || '').trim())
            .filter(Boolean)
        : [];

    const normalizedError = normalizeText(parserErrorText);
    let code = 'parser_validation_failed';
    let reason = parserErrorText || 'El parser no pudo validar esta OC';

    if (isAddressNotFoundError({ normalizedError, attemptedAddresses })) {
        code = 'address_not_found_in_customer_db';
        reason = 'No se encontro la direccion en la base de clientes';
    } else if (normalizedError.includes('rut') && normalizedError.includes('no se encuentra')) {
        code = 'rut_not_found_in_email';
        reason = 'No se encontro RUT en el correo o adjunto';
    }

    return {
        analyzed: true,
        ok: false,
        canSubmitToInvoicer: false,
        code,
        reason,
        parserStatus,
        attemptedAddresses: attemptedAddresses.length > 0 ? attemptedAddresses : null
    };
}

function buildManualOcBatchAnalysis({
    status,
    statusReason,
    error,
    dispatchAddressPrecheck
} = {}) {
    const safeStatus = String(status || '').trim();
    const canSubmitToInvoicer = canManualOcBatchEntryBeSubmitted(safeStatus);
    const precheck = dispatchAddressPrecheck && typeof dispatchAddressPrecheck === 'object'
        ? dispatchAddressPrecheck
        : null;

    let code = String(statusReason || safeStatus || 'unknown').trim() || 'unknown';
    let reason = BATCH_REASON_BY_STATUS[safeStatus] || null;

    if (precheck?.checked === true && precheck?.ok === false) {
        code = String(precheck.code || 'address_not_found_in_customer_db').trim();
        reason = String(precheck.reason || '').trim() || 'No se encontro la direccion en la base de clientes';
    }

    if (!reason && safeStatus === 'error') {
        reason = String(error || '').trim() || BATCH_REASON_BY_STATUS.error;
    }

    if (!reason) {
        reason = 'Estado de analisis no reconocido';
    }

    return {
        analyzed: true,
        ok: canSubmitToInvoicer,
        canSubmitToInvoicer,
        code,
        reason
    };
}

export {
    canManualOcBatchEntryBeSubmitted,
    buildManualOcSuccessAnalysis,
    classifyManualOcParserFailure,
    buildManualOcBatchAnalysis
};
