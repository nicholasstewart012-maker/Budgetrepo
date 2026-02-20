import * as React from 'react';
import {
    Dialog,
    DialogTrigger,
    DialogSurface,
    DialogTitle,
    DialogBody,
    DialogActions,
    DialogContent,
    Button,
    makeStyles,
    Text,
    tokens,
    shorthands
} from '@fluentui/react-components';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';
import { StatusBadge } from './StatusBadge';

const useStyles = makeStyles({
    content: {
        display: 'flex',
        flexDirection: 'column',
        gap: '20px',
        paddingTop: '16px'
    },
    section: {
        display: 'flex',
        flexDirection: 'column',
        gap: '8px',
        ...shorthands.padding('16px'),
        backgroundColor: tokens.colorNeutralBackground2,
        ...shorthands.borderRadius('4px')
    },
    row: {
        display: 'grid',
        gridTemplateColumns: '1fr 1fr',
        gap: '16px',
        '@media (max-width: 600px)': {
            gridTemplateColumns: '1fr'
        }
    },
    fieldGroup: {
        display: 'flex',
        flexDirection: 'column',
        gap: '4px'
    },
    label: {
        color: tokens.colorNeutralForeground2,
        fontWeight: 'semibold'
    },
    value: {
        color: tokens.colorNeutralForeground1
    },
    totalSection: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        backgroundColor: tokens.colorNeutralBackground3,
        ...shorthands.padding('12px'),
        ...shorthands.borderRadius('4px'),
        marginTop: '8px'
    }
});

interface IRequestDetailsModalProps {
    isOpen: boolean;
    onClose: () => void;
    request?: IConferenceRequest;
}

export const RequestDetailsModal: React.FC<IRequestDetailsModalProps> = ({ isOpen, onClose, request }) => {
    const styles = useStyles();

    if (!request) return null;

    const parsePriorities = (jsonStr?: string): string[] => {
        if (!jsonStr) return [];
        try {
            const parsed = JSON.parse(jsonStr);
            return Array.isArray(parsed) ? parsed : [];
        } catch {
            return [];
        }
    };

    const priorities = parsePriorities(request.CorporatePriorities);

    return (
        <Dialog open={isOpen} onOpenChange={(e, data) => !data.open && onClose()}>
            <DialogSurface style={{ minWidth: '600px', maxWidth: '800px' }}>
                <DialogBody>
                    <DialogTitle>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <Text size={500} weight="semibold">Request Details: {request.EventName}</Text>
                            <StatusBadge status={request.Status} />
                        </div>
                    </DialogTitle>
                    <DialogContent className={styles.content}>

                        {/* SECTION 1: Event Details */}
                        <div className={styles.section}>
                            <Text weight="bold">1. Event Details</Text>
                            <div className={styles.row}>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Event Name</Text>
                                    <Text className={styles.value}>{request.EventName || 'N/A'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Location</Text>
                                    <Text className={styles.value}>{request.EventLocation || 'N/A'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Start Date</Text>
                                    <Text className={styles.value}>{request.EventStartDate ? new Date(request.EventStartDate).toLocaleDateString() : 'N/A'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>End Date</Text>
                                    <Text className={styles.value}>{request.EventEndDate ? new Date(request.EventEndDate).toLocaleDateString() : 'N/A'}</Text>
                                </div>
                            </div>
                            <div className={styles.fieldGroup}>
                                <Text className={styles.label} size={200}>Attendees</Text>
                                <Text className={styles.value}>{request.Attendees || 'N/A'}</Text>
                            </div>
                            <div className={styles.fieldGroup}>
                                <Text className={styles.label} size={200}>How Many Attended</Text>
                                <Text className={styles.value}>{request.HowManyAttended?.toString() || 'N/A'}</Text>
                            </div>
                        </div>

                        {/* SECTION 2: Corporate Priorities */}
                        <div className={styles.section}>
                            <Text weight="bold">2. Corporate Priorities Alignment</Text>
                            <div className={styles.fieldGroup}>
                                <Text className={styles.label} size={200}>Primary Objective</Text>
                                <Text className={styles.value} wrap={true}>{request.PrimaryObjective || 'N/A'}</Text>
                            </div>
                            <div className={styles.fieldGroup}>
                                <Text className={styles.label} size={200}>Corporate Priorities Supported</Text>
                                {priorities.length > 0 ? (
                                    <ul style={{ margin: 0, paddingLeft: '20px' }}>
                                        {priorities.map(p => <li key={p} className={styles.value}>{p}</li>)}
                                    </ul>
                                ) : (
                                    <Text className={styles.value}>N/A</Text>
                                )}
                            </div>
                            <div className={styles.fieldGroup}>
                                <Text className={styles.label} size={200}>Knowledge Sharing Plan</Text>
                                <Text className={styles.value} wrap={true}>{request.KnowledgeSharingPlan || 'N/A'}</Text>
                            </div>
                            <div className={styles.fieldGroup}>
                                <Text className={styles.label} size={200}>Previously Attended</Text>
                                <Text className={styles.value} wrap={true}>{request.PreviouslyAttended || 'No'}</Text>
                            </div>
                        </div>

                        {/* SECTION 3: Costs */}
                        <div className={styles.section}>
                            <Text weight="bold">3. Total Expense / Cost Allocation</Text>
                            <div className={styles.row}>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Registration Cost</Text>
                                    <Text className={styles.value}>${request.RegistrationCost?.toFixed(2) || '0.00'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Airfare Cost</Text>
                                    <Text className={styles.value}>${request.AirfareCost?.toFixed(2) || '0.00'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Lodging Cost</Text>
                                    <Text className={styles.value}>${request.LodgingCost?.toFixed(2) || '0.00'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Meeting Room Rental Cost</Text>
                                    <Text className={styles.value}>${request.MeetingRoomRentalCost?.toFixed(2) || '0.00'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Car Rental Cost</Text>
                                    <Text className={styles.value}>${request.CarRentalCost?.toFixed(2) || '0.00'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Travel Meal Allowance</Text>
                                    <Text className={styles.value}>${request.TravelMealAllowanceCost?.toFixed(2) || '0.00'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Conference Meals Cost</Text>
                                    <Text className={styles.value}>${request.ConferenceMealsCost?.toFixed(2) || '0.00'}</Text>
                                </div>
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200}>Other Cost</Text>
                                    <Text className={styles.value}>${request.OtherCost?.toFixed(2) || '0.00'}</Text>
                                </div>
                            </div>
                            <div className={styles.totalSection}>
                                <Text weight="bold">TOTAL ESTIMATED BUDGET</Text>
                                <Text weight="bold">${request.TotalEstimatedBudget?.toFixed(2) || '0.00'}</Text>
                            </div>

                            {request.AdditionalComments && (
                                <div className={styles.fieldGroup} style={{ marginTop: '8px' }}>
                                    <Text className={styles.label} size={200}>Additional Comments</Text>
                                    <Text className={styles.value} wrap={true}>{request.AdditionalComments}</Text>
                                </div>
                            )}
                        </div>

                        {/* SECTION 4: Submitter & Notes */}
                        <div className={styles.section} style={{ backgroundColor: tokens.colorTransparentBackground }}>
                            <div className={styles.fieldGroup}>
                                <Text className={styles.label} size={200}>Submitted By</Text>
                                <Text className={styles.value}>{request.SubmitterName} ({request.SubmitterEmail})</Text>
                            </div>

                            {request.GLCode && (
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200} style={{ color: tokens.colorPaletteGreenForeground1 }}>GL Code</Text>
                                    <Text className={styles.value}>{request.GLCode}</Text>
                                </div>
                            )}

                            {request.ManagerDenialReason && (
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200} style={{ color: tokens.colorPaletteRedForeground1 }}>Manager Denial Reason</Text>
                                    <Text className={styles.value}>{request.ManagerDenialReason}</Text>
                                </div>
                            )}

                            {request.OrgDevDenialReason && (
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200} style={{ color: tokens.colorPaletteRedForeground1 }}>Org Dev Denial Reason</Text>
                                    <Text className={styles.value}>{request.OrgDevDenialReason}</Text>
                                </div>
                            )}

                            {request.AccountingDenialReason && (
                                <div className={styles.fieldGroup}>
                                    <Text className={styles.label} size={200} style={{ color: tokens.colorPaletteRedForeground1 }}>Accounting Denial Reason</Text>
                                    <Text className={styles.value}>{request.AccountingDenialReason}</Text>
                                </div>
                            )}
                        </div>

                    </DialogContent>
                    <DialogActions>
                        <DialogTrigger disableButtonEnhancement>
                            <Button appearance="secondary" onClick={onClose}>Close Details</Button>
                        </DialogTrigger>
                    </DialogActions>
                </DialogBody>
            </DialogSurface>
        </Dialog>
    );
};
