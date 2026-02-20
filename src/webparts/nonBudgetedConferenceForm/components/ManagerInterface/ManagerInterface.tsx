import * as React from 'react';
import { useState, useEffect } from 'react';
import {
    makeStyles,
    Card,
    CardHeader,
    Text,
    Button,
    Dialog,
    DialogTrigger,
    DialogSurface,
    DialogTitle,
    DialogBody,
    DialogActions,
    DialogContent,
    Textarea,
    Spinner,
    shorthands,
    tokens
} from '@fluentui/react-components';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';
import { useAppContext } from '../../../../context/AppContext';
import { StatusBadge } from '../Shared/StatusBadge';

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
    }
});

export const ManagerInterface: React.FC = () => {
    const styles = useStyles();
    const { spService, currentUser } = useAppContext();

    const [requests, setRequests] = useState<IConferenceRequest[]>([]);
    const [loading, setLoading] = useState(false);

    const [selectedRequest, setSelectedRequest] = useState<IConferenceRequest | null>(null);
    const [denialReason, setDenialReason] = useState('');
    const [isDenialDialogOpen, setIsDenialDialogOpen] = useState(false);
    const [isApprovalDialogOpen, setIsApprovalDialogOpen] = useState(false);

    const loadRequests = async () => {
        setLoading(true);
        try {
            // Filter by requests where ManagerEmail matches current user
            const filter = `ManagerEmail eq '${currentUser.email}' and Status eq 'Pending Manager Approval'`;
            const result = await spService.getRequests(filter);
            setRequests(result);
        } catch (error) {
            console.error('Error loading manager queue:', error);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void loadRequests();
    }, [currentUser.email]);

    const handleApprove = async () => {
        if (!selectedRequest || !selectedRequest.Id) return;
        try {
            await spService.updateRequest(selectedRequest.Id, {
                Status: 'Pending Org Dev Approval',
                ManagerApprovalDate: new Date().toISOString()
            });
            setIsApprovalDialogOpen(false);
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
                ManagerDenialReason: denialReason,
                ManagerApprovalDate: new Date().toISOString()
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
        setIsApprovalDialogOpen(true);
    };

    const openDenyDialog = (req: IConferenceRequest) => {
        setSelectedRequest(req);
        setDenialReason('');
        setIsDenialDialogOpen(true);
    };

    return (
        <div className={styles.root}>
            <Text size={500} weight="semibold">Manager Approval Queue</Text>

            {loading && <Spinner label="Loading pending requests..." />}

            {!loading && requests.length === 0 && (
                <Text>You have no pending requests to review.</Text>
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
                        description={<Text>Submitted by: {req.SubmitterName}</Text>}
                    />
                    <div style={{ padding: '0 12px 12px 12px' }}>
                        <Text block><strong>Location:</strong> {req.EventLocation}</Text>
                        <Text block><strong>Total Est. Budget:</strong> ${req.TotalEstimatedBudget?.toFixed(2) || '0.00'}</Text>
                        <Text block><strong>Attendees:</strong> {req.Attendees}</Text>

                        <div className={styles.actions}>
                            <Button appearance="primary" onClick={() => openApproveDialog(req)}>Approve</Button>
                            <Button appearance="secondary" onClick={() => openDenyDialog(req)}>Deny</Button>
                        </div>
                    </div>
                </Card>
            ))}

            {/* Approve Dialog */}
            <Dialog open={isApprovalDialogOpen} onOpenChange={(e, data) => setIsApprovalDialogOpen(data.open)}>
                <DialogSurface>
                    <DialogBody>
                        <DialogTitle>Approve Request</DialogTitle>
                        <DialogContent>
                            <Text>
                                By approving this request, I confirm that this event aligns with departmental goals and that budget implications have been considered.
                            </Text>
                        </DialogContent>
                        <DialogActions>
                            <Button appearance="secondary" onClick={() => setIsApprovalDialogOpen(false)}>Cancel</Button>
                            <Button appearance="primary" onClick={handleApprove}>Approve</Button>
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
                            <Text>Please provide a reason for denying this request.</Text>
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
        </div>
    );
};
