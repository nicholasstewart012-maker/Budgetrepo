import * as React from 'react';
import { useState, useEffect } from 'react';
import {
    makeStyles,
    Card,
    CardHeader,
    Text,
    Button,
    Dialog,
    DialogSurface,
    DialogTitle,
    DialogBody,
    DialogActions,
    DialogContent,
    Textarea,
    Input,
    Label,
    Spinner,
    shorthands,
    tokens
} from '@fluentui/react-components';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';
import { useAppContext } from '../../../../context/AppContext';
import { StatusBadge } from '../Shared/StatusBadge';
import { RequestDetailsModal } from '../Shared/RequestDetailsModal';

const useStyles = makeStyles({
    root: {
        display: 'flex',
        flexDirection: 'column',
        gap: '16px'
    },
    card: {
        ...shorthands.margin('0px'),
        width: '100%'
    },
    actions: {
        display: 'flex',
        gap: '8px',
        marginTop: '16px',
        borderTop: `1px solid ${tokens.colorNeutralStroke1}`,
        paddingTop: '16px'
    },
    dialogContent: {
        display: 'flex',
        flexDirection: 'column',
        gap: '16px'
    },
    row: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        width: '100%'
    }
});

export const AccountingInterface: React.FC = () => {
    const styles = useStyles();
    const { spService, currentUser } = useAppContext();

    const [requests, setRequests] = useState<IConferenceRequest[]>([]);
    const [loading, setLoading] = useState(false);

    const [selectedRequest, setSelectedRequest] = useState<IConferenceRequest | undefined>(undefined);

    const [denialReason, setDenialReason] = useState('');
    const [isDenialDialogOpen, setIsDenialDialogOpen] = useState(false);

    const [glCode, setGlCode] = useState('');
    const [isApprovalDialogOpen, setIsApprovalDialogOpen] = useState(false);

    // View Details State
    const [isDetailsModalOpen, setIsDetailsModalOpen] = useState(false);

    const loadRequests = async () => {
        setLoading(true);
        try {
            const filter = `Status eq 'Pending Accounting Approval'`;
            const result = await spService.getRequests(filter);
            setRequests(result);
        } catch (error) {
            console.error('Error loading accounting queue:', error);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void loadRequests();
    }, []);

    const handleApprove = async () => {
        if (!selectedRequest || !selectedRequest.Id || !glCode.trim()) return;
        try {
            await spService.updateRequest(selectedRequest.Id, {
                Status: 'Fully Approved',
                AccountingApproverEmail: currentUser.email,
                AccountingApprovalDate: new Date().toISOString(),
                GLCode: glCode.trim()
            });
            setIsApprovalDialogOpen(false);
            setGlCode('');
            void loadRequests();
        } catch (error) {
            console.error('Error approving request', error);
        }
    };

    const handleDeny = async () => {
        if (!selectedRequest || !selectedRequest.Id || !denialReason) return;
        try {
            await spService.updateRequest(selectedRequest.Id, {
                Status: 'Denied',
                AccountingDenialReason: denialReason,
                AccountingApproverEmail: currentUser.email,
                AccountingApprovalDate: new Date().toISOString()
            });
            setIsDenialDialogOpen(false);
            setDenialReason('');
            void loadRequests();
        } catch (error) {
            console.error('Error denying request', error);
        }
    };

    const openApproveDialog = (req: IConferenceRequest) => {
        setSelectedRequest(req);
        setGlCode('');
        setIsApprovalDialogOpen(true);
    };

    const openDenyDialog = (req: IConferenceRequest) => {
        setSelectedRequest(req);
        setDenialReason('');
        setIsDenialDialogOpen(true);
    };

    return (
        <div className={styles.root}>
            <Text size={500} weight="semibold">Accounting Final Review Queue</Text>

            {loading && <Spinner label="Loading pending requests..." />}

            {!loading && requests.length === 0 && (
                <Text>There are no requests pending Accounting approval.</Text>
            )}

            {requests.map(req => (
                <Card key={req.Id} className={styles.card}>
                    <CardHeader
                        header={
                            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                                <Text weight="semibold">{req.EventName}</Text>
                                <StatusBadge status={req.Status} />
                            </div>
                        }
                        description={
                            <>
                                <Text block>Submitted by: {req.SubmitterName}</Text>
                                <div className={styles.row}>
                                    <Text size={300}>
                                        {new Date(req.EventStartDate || '').toLocaleDateString()} - {new Date(req.EventEndDate || '').toLocaleDateString()}
                                    </Text>
                                    <Button appearance="subtle" onClick={() => { setSelectedRequest(req); setIsDetailsModalOpen(true); }}>
                                        View Details
                                    </Button>
                                </div>
                            </>
                        }
                    />
                    <div style={{ padding: '0 12px 12px 12px' }}>
                        <Text block><strong>Location:</strong> {req.EventLocation}</Text>
                        <Text block><strong>Dates:</strong> {new Date(req.EventStartDate || '').toLocaleDateString()} - {new Date(req.EventEndDate || '').toLocaleDateString()}</Text>
                        <Text block><strong>Total Est. Budget:</strong> ${req.TotalEstimatedBudget?.toFixed(2) || '0.00'}</Text>
                        <Text block><strong>Attendees:</strong> {req.Attendees}</Text>

                        <div className={styles.actions}>
                            <Button appearance="primary" onClick={() => openApproveDialog(req)}>Approve & Assign GL</Button>
                            <Button appearance="secondary" onClick={() => openDenyDialog(req)}>Deny</Button>
                        </div>
                    </div>
                </Card>
            ))}

            {/* Approve Dialog */}
            <Dialog open={isApprovalDialogOpen} onOpenChange={(e, data) => setIsApprovalDialogOpen(data.open)}>
                <DialogSurface>
                    <DialogBody>
                        <DialogTitle>Final Approval</DialogTitle>
                        <DialogContent className={styles.dialogContent}>
                            <Text>Assign a GL Code / Misc. Event Code for this request to complete final approval.</Text>
                            <div>
                                <Label required>GL Code</Label>
                                <Input
                                    value={glCode}
                                    onChange={(e) => setGlCode(e.target.value)}
                                    placeholder="e.g. 1234-567-890"
                                    style={{ width: '100%' }}
                                />
                            </div>
                        </DialogContent>
                        <DialogActions>
                            <Button appearance="secondary" onClick={() => setIsApprovalDialogOpen(false)}>Cancel</Button>
                            <Button appearance="primary" disabled={!glCode.trim()} onClick={handleApprove}>Approve Request</Button>
                        </DialogActions>
                    </DialogBody>
                </DialogSurface>
            </Dialog>

            {/* Deny Dialog */}
            <Dialog open={isDenialDialogOpen} onOpenChange={(e, data) => setIsDenialDialogOpen(data.open)}>
                <DialogSurface>
                    <DialogBody>
                        <DialogTitle>Deny Request</DialogTitle>
                        <DialogContent className={styles.dialogContent}>
                            <Text>Please provide a reason for denying this request. This will be visible to the submitter.</Text>
                            <Textarea
                                value={denialReason}
                                onChange={(e) => setDenialReason(e.target.value)}
                                placeholder="Reason for denial..."
                            />
                        </DialogContent>
                        <DialogActions>
                            <Button appearance="secondary" onClick={() => setIsDenialDialogOpen(false)}>Cancel</Button>
                            <Button appearance="primary" disabled={!denialReason} onClick={handleDeny}>Confirm Denial</Button>
                        </DialogActions>
                    </DialogBody>
                </DialogSurface>
            </Dialog>

            <RequestDetailsModal
                isOpen={isDetailsModalOpen}
                onClose={() => setIsDetailsModalOpen(false)}
                request={selectedRequest}
            />
        </div>
    );
};
