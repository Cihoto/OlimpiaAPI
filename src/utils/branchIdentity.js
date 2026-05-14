const DIACRITICS_REGEX = new RegExp('[\\u0300-\\u036f]', 'g');

// Normaliza el nombre de una sucursal para comparación robusta:
// quita acentos, pasa a minúsculas, colapsa whitespace y hace trim.
// El resultado se usa como parte de la identidad de sucursal para
// detectar despachos duplicados (cliente + sucursal + fecha).
function normalizeBranchName(value) {
    return String(value || '')
        .normalize('NFD')
        .replace(DIACRITICS_REGEX, '')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();
}

export { normalizeBranchName };
