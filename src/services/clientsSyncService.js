// Orquesta sincronización de catálogo de clientes Defontana -> Mongo cache.
// Lista de RUTs excluidos (giros genéricos que ensucian reportes).

import { getAllClients } from './accountingDefontanaService.js';
import { saveClients, listClients, getCacheStatus } from './mongoClientsCache.js';

// Centralizado para que toda Vista 1 + Vista 2 use el mismo set.
export const EXCLUDED_RUTS = [
    '11.111.111-1', // GIRO GENERICO - no aporta a contabilidad real
];

function normalizeRut(rut) {
    if (!rut) return '';
    return String(rut).replace(/\./g, '').replace(/\s/g, '').toUpperCase();
}

const EXCLUDED_NORM = EXCLUDED_RUTS.map(normalizeRut);

export function isExcludedRut(rut) {
    return EXCLUDED_NORM.includes(normalizeRut(rut));
}

// Refresh forzado contra Defontana.
export async function refreshClientsCache(apiKey) {
    const raw = await getAllClients(apiKey);
    const filtered = raw.filter(c => !isExcludedRut(c.legalCode || c.fileID));
    const result = await saveClients(filtered);
    return { ...result, excluded: raw.length - filtered.length };
}

// Devuelve clientes del cache. Si está vacío o vieja según TTL, sincroniza primero.
export async function getClientsOrSync(apiKey, { force = false } = {}) {
    const status = await getCacheStatus();
    if (force || !status.synced || status.isStale) {
        await refreshClientsCache(apiKey);
    }
    return listClients({ excludeRuts: EXCLUDED_RUTS });
}

// Solo cache, no toca Defontana.
export async function getCachedClients() {
    return listClients({ excludeRuts: EXCLUDED_RUTS });
}

export { getCacheStatus };
