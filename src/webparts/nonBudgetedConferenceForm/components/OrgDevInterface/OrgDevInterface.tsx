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

export const OrgDevInterface: React.FC = () => {
    const styles = useStyles();
    const { spService, currentUser } = useAppContext();

    const [requests, setRequests] = useState<IConferenceRequest[]>([]);
    const [loading, setLoading] = useState(false);

    const [selectedRequest, setSelectedRequest] = useState<IConferenceRequest | undefined>(undefined);
    const [denialReason, setDenialReason] = useState('');
    const [isDenialDialogOpen, setIsDenialDialogOpen] = useState(false);
    const [isApprovalDialogOpen, setIsApprovalDialogOpen] = useState(false);
    // View Details State
    const [isDetailsModalOpen, setIsDetailsModalOpen] = useState(false);

    const loadRequests = async () => {
        setLoading(true);
        try {
            const filter = `Status eq 'Pending Org Dev Approval'`;
            const result = await spService.getRequests(filter);
            setRequests(result);
        } catch (error) {
            console.error('Error loading org dev queue:', error);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void loadRequests();
    }, []);

    const handleApprove = async (req: IConferenceRequest) => {
        if (!req || !req.Id) return;
        try {
            await spService.updateRequest(req.Id, {
                Status: 'Pending Accounting Approval',
                OrgDevApproverEmail: currentUser.email,
                OrgDevApprovalDate: new Date().toISOString()
            });
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
                OrgDevDenialReason: denialReason,
                OrgDevApproverEmail: currentUser.email,
                OrgDevApprovalDate: new Date().toISOString()
            });
            setIsDenialDialogOpen(false);
            setDenialReason('');
            void loadRequests();
        } catch (error) {
            console.error('Error denying request', error);
        }
    };

    const openDenyDialog = (req: IConferenceRequest) => {
        setSelectedRequest(req);
        setDenialReason('');
        setIsDenialDialogOpen(true);
    };

    return (
        <div className={styles.root}>
            <Text size={500} weight="semibold">Organization Development Review Queue</Text>

            {loading && <Spinner label="Loading pending requests..." />}

            {!loading && requests.length === 0 && (
                <Text>There are no requests pending Org Dev approval.</Text>
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
                        <Text block><strong>Primary Objective:</strong> {req.PrimaryObjective}</Text>

                        <div className={styles.actions}>
                            <Button appearance="primary" onClick={() => handleApprove(req)}>Approve</Button>
                            <Button appearance="secondary" onClick={() => openDenyDialog(req)}>Deny</Button>
                        </div>
                    </div>
                </Card>
            ))}

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
