import {
  faBuildingColumns,
  faCalendarDays,
  faEnvelope,
  faFileLines,
  faFolderOpen,
  faGear,
  faMagnifyingGlass,
  faMoneyBillWave,
  faPeopleGroup,
  faPhone,
  faShieldHalved,
  faWandSparkles,
} from '@fortawesome/free-solid-svg-icons'

export const ADMIN_SETTINGS_STORAGE_KEY = 'spark_admin_settings'

export const DEFAULT_SETTINGS = {
  assistantName: 'Spark',
  brandColor: '#0E2D6D',
  brandColorDark: '#0a1f4e',
  brandColorLight: '#EBF0F8',
  brandGold: '#CFB87C',
  brandGoldWarm: '#F9A608',
  brandOrange: '#EA5A00',
  brandSuccess: '#107c10',
  brandInfo: '#0078d4',
  disclaimer: 'Spark is here to help you. When asking a question, be specific and detailed so Spark can give the best response.',
  notFound: 'Spark was unable to find an answer.',
  splashCards: [],
}

export const DEFAULT_SPLASH_CARD_PRESETS = [
  {
    id: 'preset-travel',
    icon: 'calendar',
    category: 'Travel',
    question: 'What is the current business travel and hotel policy?',
  },
  {
    id: 'preset-compliance',
    icon: 'shield',
    category: 'Compliance',
    question: 'How do I report an ethics or compliance concern?',
  },
  {
    id: 'preset-access',
    icon: 'people',
    category: 'Access',
    question: 'How do I request access to a shared drive or application?',
  },
]

export const SPLASH_ICON_OPTIONS = [
  { value: 'sparkles', label: 'Sparkles', icon: faWandSparkles },
  { value: 'shield', label: 'Shield', icon: faShieldHalved },
  { value: 'mail', label: 'Mail', icon: faEnvelope },
  { value: 'folder', label: 'Folder', icon: faFolderOpen },
  { value: 'search', label: 'Search', icon: faMagnifyingGlass },
  { value: 'people', label: 'People', icon: faPeopleGroup },
  { value: 'document', label: 'Document', icon: faFileLines },
  { value: 'calendar', label: 'Calendar', icon: faCalendarDays },
  { value: 'phone', label: 'Phone', icon: faPhone },
  { value: 'settings', label: 'Settings', icon: faGear },
  { value: 'money', label: 'Money', icon: faMoneyBillWave },
  { value: 'bank', label: 'Bank', icon: faBuildingColumns },
]

export function getIconOption(value) {
  return SPLASH_ICON_OPTIONS.find(option => option.value === value) || SPLASH_ICON_OPTIONS[0]
}

export function normalizeSettings(settings = {}) {
  return {
    ...DEFAULT_SETTINGS,
    ...settings,
    splashCards: Array.isArray(settings?.splashCards) ? settings.splashCards : DEFAULT_SETTINGS.splashCards,
  }
}

export function loadPersistedSettings() {
  if (typeof window === 'undefined') {
    return DEFAULT_SETTINGS
  }

  try {
    const raw = window.localStorage.getItem(ADMIN_SETTINGS_STORAGE_KEY)
    if (!raw) return DEFAULT_SETTINGS
    return normalizeSettings(JSON.parse(raw))
  } catch {
    return DEFAULT_SETTINGS
  }
}

export function buildThemeVars(settings = {}) {
  const resolved = normalizeSettings(settings)

  return {
    '--brand-primary': resolved.brandColor,
    '--brand-primary-600': resolved.brandColorDark,
    '--brand-accent': resolved.brandGold,
    '--brand-accent-dim': resolved.brandGoldWarm,
    '--brand-cream': resolved.brandColorLight,
    '--brand-surface': '#ffffff',
    '--brand-bg': '#f8f9fc',
    '--brand-accent-weak': 'rgba(249, 166, 8, 0.16)',
    '--brand-accent-strong': 'rgba(249, 166, 8, 0.4)',
    '--dark': resolved.brandColor,
    '--mid': resolved.brandColorDark,
    '--accent': resolved.brandGoldWarm,
    '--gold': resolved.brandGold,
    '--gold-dim': resolved.brandGoldWarm,
    '--cream': resolved.brandColorLight,
    '--white': '#ffffff',
    '--bg': '#f8f9fc',
    '--text': '#323130',
    '--muted': '#605e5c',
    '--faint': '#a19f9d',
    '--border': '#edebe9',
    '--border2': '#c8c6c4',
    '--shadow': '0 1px 3px rgba(0,0,0,.06), 0 4px 16px rgba(0,0,0,.07)',
    '--focus-ring': 'rgba(14,45,109,0.12)',
    '--rnst-navy': resolved.brandColor,
    '--rnst-navy-dark': resolved.brandColorDark,
    '--rnst-navy-light': resolved.brandColorLight,
    '--rnst-gold': resolved.brandGold,
    '--rnst-gold-warm': resolved.brandGoldWarm,
    '--rnst-orange': resolved.brandOrange,
    '--rnst-success': resolved.brandSuccess,
    '--rnst-success-bg': '#dff6dd',
    '--rnst-error': '#d13438',
    '--rnst-error-bg': '#fde7e9',
    '--rnst-warning': '#7a5c00',
    '--rnst-warning-bg': '#fff4ce',
    '--rnst-info': resolved.brandInfo,
    '--rnst-info-bg': '#deecf9',
    '--rnst-text-primary': '#323130',
    '--rnst-text-secondary': '#605e5c',
    '--rnst-text-disabled': '#a19f9d',
    '--rnst-border': '#edebe9',
    '--rnst-border-strong': '#c8c6c4',
    '--rnst-surface': '#ffffff',
    '--rnst-surface-alt': '#f8f9fc',
    '--rnst-surface-hover': '#f3f2f1',
    '--rnst-shadow-sm': '0 2px 4px rgba(0,0,0,0.06)',
    '--rnst-shadow-md': '0 4px 16px rgba(0,0,0,0.10)',
    '--rnst-shadow-lg': '0 8px 32px rgba(14,45,109,0.15)',
    '--rnst-radius-sm': '4px',
    '--rnst-radius-md': '8px',
    '--rnst-radius-lg': '16px',
    '--rnst-radius-pill': '100px',
    '--rnst-font-display': "'Libre Baskerville', Georgia, serif",
    '--rnst-font-body': "'DM Sans', 'Segoe UI', sans-serif",
  }
}
