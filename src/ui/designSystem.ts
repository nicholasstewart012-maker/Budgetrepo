import { STATUSES } from '../constants';

export const CategoryColorMap: Record<string, string> = {
    'Retail': '#0E2D6D',
    'Compliance Courses': '#F9A608',
    'Leadership': '#EA5400',
    'Customer & Emp Opp. Webinars': '#96D4E8',
    'Commercial & Credit': '#0073B1',
    'Other': '#EC008C'
};

export function getCategoryColor(category?: string): string | undefined {
    if (!category) return undefined;
    return CategoryColorMap[category];
}

export function getReadableTextColor(bgHex: string): '#FFFFFF' | '#111111' {
    if (!bgHex) return '#111111';
    const hex = bgHex.replace('#', '');
    const r = parseInt(hex.substr(0, 2), 16);
    const g = parseInt(hex.substr(2, 2), 16);
    const b = parseInt(hex.substr(4, 2), 16);
    const yiq = ((r * 299) + (g * 587) + (b * 114)) / 1000;
    return (yiq >= 128) ? '#111111' : '#FFFFFF';
}

export function getStatusColor(status?: string): string {
    switch (status) {
        case STATUSES.PENDING_MANAGER:
            return '#96D4E8'; // RNST Sky Blue
        case STATUSES.PENDING_ORG_DEV:
            return '#EA5400'; // RNST Orange (FIXED to EA5400)
        case STATUSES.PENDING_ACCOUNTING:
            return '#F9A608'; // RNST Yellow
        case STATUSES.FULLY_APPROVED:
            return '#107C10'; // Fluent Green
        case STATUSES.DENIED:
            return '#D13438'; // Fluent Red
        case STATUSES.DRAFT:
        default:
            return '#E0E0E0'; // Neutral
    }
}
