import * as React from 'react';
import { useState, useEffect } from 'react';
import {
    makeStyles,
    Card,
    Text,
    Button,
    Textarea,
    Spinner,
    shorthands,
    tokens
} from '@fluentui/react-components';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';
import { useAppContext } from '../../../../context/AppContext';
import { StatusBadge } from '../Shared/StatusBadge';
import { RequestDetailsModal } from '../Shared/RequestDetailsModal';
import { SpfxModal } from '../Shared/SpfxModal';

const useStyles = makeStyles({
    root: {
        display: 'flex',
        flexDirection: 'column',
        gap: '16px',
    },
    card: {
        ...shorthands.margin('0px'),
        width: '100%',
        boxShadow: tokens.shadow2,
        border: `1px solid ${tokens.colorNeutralStroke1}`
    },
    row: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        paddingBottom: '8px',
        borderBottom: `1px solid ${tokens.colorNeutralStroke2}`
    },
    metaRow: {
        display: 'flex',
        gap: '24px',
        flexWrap: 'wrap',
        paddingTop: '12px'
    },
    metricBlock: {
        display: 'flex',
        flexDirection: 'column',
        gap: '2px'
    },
    metricLabel: {
        fontSize: '11px',
        color: tokens.colorNeutralForeground3,
        textTransform: 'uppercase',
        letterSpacing: '0.5px',
        fontWeight: 'semibold'
    },
    metricValue: {
        fontSize: '14px',
        color: tokens.colorNeutralForeground1,
        fontWeight: 'semibold'
    },
    actions: {
        display: 'flex',
        justifyContent: 'flex-end',
        gap: '8px',
        marginTop: '16px',
        paddingTop: '16px',
        borderTop: `1px dashed ${tokens.colorNeutralStroke2}`
    },
    acknowledgement: {
        backgroundColor: '#f0f4ff',
        border: '1px solid #c7d4f5',
        borderRadius: '8px',
        padding: '12px 16px',
        fontSize: '13px',
        color: '#333',
        lineHeight: '1.5',
        marginBottom: '8px',
    },
    formField: {
        display: 'flex',
        flexDirection: 'column',
        gap: '6px',
    },
    textarea: {
        minHeight: '120px',
    }
});

export const ManagerInterface: React.FC = () => {
    const styles = useStyles();
    const { spService, currentUser } = useAppContext();

    const [requests, setRequests] = useState<IConferenceRequest[]>([]);
    const [loading, setLoading] = useState(false);
    const [selectedRequest, setSelectedRequest] = useState<IConferenceRequest | undefined>(undefined);
    const [denialReason, setDenialReason] = useState('');
    const [isDenialOpen, setIsDenialOpen] = useState(false);
    const [isApprovalOpen, setIsApprovalOpen] = useState(false);
    const [isDetailsOpen, setIsDetailsOpen] = useState(false);

    const loadRequests = async () => {
        setLoading(true);
        try {
            const filter = `ManagerEmail eq '${currentUser.email}' and Status eq 'Pending Manager Approval'`;
            const result = await spService.getRequests(filter);
            setRequests(result);
        } catch (error) {
            console.error('Error loading manager queue:', error);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => { void loadRequests(); }, [currentUser.email]);

    const handleApprove = async () => {
        if (!selectedRequest?.Id) return;
        try {
            await spService.updateRequest(selectedRequest.Id, {
                Status: 'Pending Org Dev Approval',
                ManagerApprovalDate: new Date().toISOString()
            });
            setIsApprovalOpen(false);
            void loadRequests();
        } catch (error) {
            console.error('Error approving request', error);
        }
    };

    const handleDeny = async () => {
        if (!selectedRequest?.Id || !denialReason) return;
        try {
            await spService.updateRequest(selectedRequest.Id, {
                Status: 'Denied',
                ManagerDenialReason: denialReason,
                ManagerApprovalDate: new Date().toISOString()
            });
            setIsDenialOpen(false);
            setDenialReason('');
            void loadRequests();
        } catch (error) {
            console.error('Error denying request', error);
        }
    };

    return (
        <div className={styles.root}>
            <Text size={500} weight="semibold">Manager Approval Queue</Text>

            {loading && <Spinner label="Loading pending requests..." />}

            {!loading && requests.length === 0 && (
                <div style={{ textAlign: 'center', padding: '40px', color: tokens.colorNeutralForeground3 }}>
                    <Text size={400}>You have no pending requests to review.</Text>
                </div>
            )}

            {requests.map(req => (
                <Card key={req.Id} className={styles.card}>
                    <div style={{ padding: '16px' }}>
                        <div className={styles.row}>
                            <Text weight="bold" size={400}>{req.EventName}</Text>
                            <StatusBadge status={req.Status} />
                        </div>
                        <div className={styles.metaRow}>
                            <div className={styles.metricBlock}>
                                <Text className={styles.metricLabel}>SUBMITTED BY</Text>
                                <Text className={styles.metricValue}>{req.SubmitterName}</Text>
                            </div>
                            <div className={styles.metricBlock}>
                                <Text className={styles.metricLabel}>DATES</Text>
                                <Text className={styles.metricValue}>
                                    {req.EventStartDate ? new Date(req.EventStartDate).toLocaleDateString() : 'TBD'} -&nbsp;
                                    {req.EventEndDate ? new Date(req.EventEndDate).toLocaleDateString() : 'TBD'}
                                </Text>
                            </div>
                            <div className={styles.metricBlock}>
                                <Text className={styles.metricLabel}>LOCATION</Text>
                                <Text className={styles.metricValue}>{req.EventLocation}</Text>
                            </div>
                            <div className={styles.metricBlock}>
                                <Text className={styles.metricLabel}>TOTAL BUDGET</Text>
                                <Text className={styles.metricValue}>${req.TotalEstimatedBudget?.toFixed(2) || '0.00'}</Text>
                            </div>
                            <div className={styles.metricBlock}>
                                <Text className={styles.metricLabel}>ATTENDEES</Text>
                                <Text className={styles.metricValue}>{req.Attendees}</Text>
                            </div>
                        </div>
                        <div className={styles.actions}>
                            <Button appearance="subtle" onClick={() => { setSelectedRequest(req); setIsDetailsOpen(true); }}>
                                View Details
                            </Button>
                            <Button appearance="secondary" onClick={() => { setSelectedRequest(req); setDenialReason(''); setIsDenialOpen(true); }}>
                                Deny
                            </Button>
                            <Button appearance="primary" onClick={() => { setSelectedRequest(req); setIsApprovalOpen(true); }}>
                                Approve
                            </Button>
                        </div>
                    </div>
                </Card>
            ))}

            {/* ── APPROVE MODAL ── */}
            <SpfxModal
                isOpen={isApprovalOpen}
                onClose={() => setIsApprovalOpen(false)}
                title="Approve Request"
                footer={
                    <>
                        <Button appearance="secondary" onClick={() => setIsApprovalOpen(false)}>Cancel</Button>
                        <Button appearance="primary" onClick={handleApprove}>Approve</Button>
                    </>
                }
            >
                <div className={styles.acknowledgement}>
                    By approving this request, I confirm that this event aligns with departmental goals
                    and that budget implications have been considered.
                </div>
                {selectedRequest && (
                    <div style={{ marginTop: '8px', color: '#616161', fontSize: '13px' }}>
                        <strong>{selectedRequest.EventName}</strong> — submitted by {selectedRequest.SubmitterName}
                    </div>
                )}
            </SpfxModal>

            {/* ── DENY MODAL ── */}
            <SpfxModal
                isOpen={isDenialOpen}
                onClose={() => setIsDenialOpen(false)}
                title="Deny Request"
                subtitle="Please provide a reason for denying. This will be visible to the submitter."
                footer={
                    <>
                        <Button appearance="secondary" onClick={() => setIsDenialOpen(false)}>Cancel</Button>
                        <Button appearance="primary" disabled={!denialReason} onClick={handleDeny}>
                            Confirm Denial
                        </Button>
                    </>
                }
            >
                <div className={styles.formField}>
                    <Textarea
                        className={styles.textarea}
                        value={denialReason}
                        onChange={(_e, data) => setDenialReason(data.value)}
                        placeholder="Reason for denial..."
                    />
                </div>
            </SpfxModal>

            <RequestDetailsModal
                isOpen={isDetailsOpen}
                onClose={() => setIsDetailsOpen(false)}
                request={selectedRequest}
            />
        </div>
    );
};
