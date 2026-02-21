import * as React from 'react';
import * as ReactDOM from 'react-dom';
import { Button, Text, tokens } from '@fluentui/react-components';
import { Dismiss24Regular } from '@fluentui/react-icons';

// ─────────────────────────────────────────────────────────────────────────────
// SpfxModal — Custom portal modal for SPFx environments.
//
// Why: Fluent UI v9 Dialog uses React portals that exit the FluentProvider
// tree, losing all theme context (backdrop, tokens, shadows). In SPFx this
// manifests as missing backdrops, wrong positioning, or dialogs rendering at
// the top of the page. This component bypasses all of that by using
// ReactDOM.createPortal directly into document.body with 100% inline styles —
// zero dependency on Fluent UI context for layout/backdrop.
//
// Fluent UI components (Button, Input, Textarea, etc.) can still be used
// INSIDE the modal body and will style correctly because the FluentProvider
// renders inline CSS variables onto document.documentElement, not just inside
// its subtree. Only the Dialog shell/backdrop was broken — this fixes that.
// ─────────────────────────────────────────────────────────────────────────────

interface ISpfxModalProps {
    isOpen: boolean;
    onClose: () => void;
    title: string;
    subtitle?: string;
    width?: number;
    showCloseButton?: boolean;
    children: React.ReactNode;
    footer?: React.ReactNode;
}

export const SpfxModal: React.FC<ISpfxModalProps> = ({
    isOpen,
    onClose,
    title,
    subtitle,
    width = 560,
    showCloseButton = true,
    children,
    footer
}) => {
    // Close on Escape key
    React.useEffect(() => {
        if (!isOpen) return;
        const handleKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose();
        };
        document.addEventListener('keydown', handleKey);
        return () => document.removeEventListener('keydown', handleKey);
    }, [isOpen, onClose]);

    // Lock body scroll while open
    React.useEffect(() => {
        if (isOpen) {
            document.body.style.overflow = 'hidden';
        } else {
            document.body.style.overflow = '';
        }
        return () => { document.body.style.overflow = ''; };
    }, [isOpen]);

    if (!isOpen) return null;

    const modal = (
        <>
            {/* ── BACKDROP ── */}
            <div
                onClick={onClose}
                style={{
                    position: 'fixed',
                    inset: 0,
                    backgroundColor: 'rgba(0, 0, 0, 0.50)',
                    zIndex: 9999998,
                    backdropFilter: 'blur(2px)',
                    WebkitBackdropFilter: 'blur(2px)',
                }}
                aria-hidden="true"
            />

            {/* ── MODAL PANEL ── */}
            <div
                role="dialog"
                aria-modal="true"
                aria-labelledby="spfx-modal-title"
                style={{
                    position: 'fixed',
                    inset: 0,
                    zIndex: 9999999,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    padding: '16px',
                    pointerEvents: 'none', // let backdrop clicks through
                }}
            >
                <div
                    onClick={e => e.stopPropagation()}
                    style={{
                        pointerEvents: 'all',
                        backgroundColor: '#ffffff',
                        borderRadius: '12px',
                        boxShadow: '0 32px 64px rgba(0,0,0,0.24), 0 8px 24px rgba(0,0,0,0.12)',
                        width: '100%',
                        maxWidth: `${width}px`,
                        maxHeight: '90vh',
                        display: 'flex',
                        flexDirection: 'column',
                        overflow: 'hidden',
                        fontFamily: "'Segoe UI', 'Segoe UI Web (West European)', -apple-system, BlinkMacSystemFont, sans-serif",
                    }}
                >
                    {/* HEADER */}
                    <div style={{
                        padding: '24px 24px 16px 24px',
                        borderBottom: '1px solid #e0e0e0',
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'flex-start',
                        flexShrink: 0,
                    }}>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', paddingRight: '16px' }}>
                            <span
                                id="spfx-modal-title"
                                style={{
                                    fontSize: '18px',
                                    fontWeight: '700',
                                    color: '#141414',
                                    lineHeight: '1.3',
                                }}
                            >
                                {title}
                            </span>
                            {subtitle && (
                                <span style={{ fontSize: '13px', color: '#616161', lineHeight: '1.4' }}>
                                    {subtitle}
                                </span>
                            )}
                        </div>
                        {showCloseButton && (
                            <button
                                onClick={onClose}
                                aria-label="Close dialog"
                                style={{
                                    background: 'none',
                                    border: 'none',
                                    cursor: 'pointer',
                                    padding: '4px',
                                    borderRadius: '4px',
                                    color: '#616161',
                                    display: 'flex',
                                    alignItems: 'center',
                                    flexShrink: 0,
                                }}
                            >
                                <Dismiss24Regular />
                            </button>
                        )}
                    </div>

                    {/* BODY — scrollable */}
                    <div style={{
                        padding: '20px 24px',
                        overflowY: 'auto',
                        flex: 1,
                    }}>
                        {children}
                    </div>

                    {/* FOOTER */}
                    {footer && (
                        <div style={{
                            padding: '16px 24px',
                            borderTop: '1px solid #e0e0e0',
                            display: 'flex',
                            justifyContent: 'flex-end',
                            gap: '8px',
                            flexShrink: 0,
                            backgroundColor: '#fafafa',
                        }}>
                            {footer}
                        </div>
                    )}
                </div>
            </div>
        </>
    );

    // Portal directly into document.body — no FluentProvider context needed
    // for backdrop/positioning. Fluent CSS vars on :root still apply inside.
    return ReactDOM.createPortal(modal, document.body);
};
