import * as React from 'react';
import {
    makeStyles,
    Text,
    Button,
    tokens,
    shorthands,
    Divider
} from '@fluentui/react-components';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';
import { StatusBadge } from './StatusBadge';
import { SpfxModal } from './SpfxModal';

const useStyles = makeStyles({
    content: {
        display: 'flex',
        flexDirection: 'column',
        gap: '20px',
    },
    section: {
        display: 'flex',
        flexDirection: 'column',
        gap: '12px',
        ...shorthands.padding('20px'),
        backgroundColor: tokens.colorNeutralBackground2,
        ...shorthands.borderRadius('8px'),
        border: `1px solid ${tokens.colorNeutralStroke1}`
    },
    row: {
        display: 'grid',
        gridTemplateColumns: '1fr 1fr',
        gap: '16px',
    },
    fieldGroup: {
        display: 'flex',
        flexDirection: 'column',
        gap: '4px'
    },
    label: {
        color: tokens.colorNeutralForeground2,
        fontWeight: 'semibold',
        fontSize: '11px',
        textTransform: 'uppercase',
        letterSpacing: '0.5px'
    },
    value: {
        color: tokens.colorNeutralForeground1,
        fontSize: '14px'
    },
    totalSection: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        backgroundColor: tokens.colorNeutralBackground3,
        ...shorthands.padding('14px', '16px'),
        ...shorthands.borderRadius('6px'),
        marginTop: '4px'
    },
    priorityList: {
        margin: '4px 0 0 0',
        paddingLeft: '18px',
        display: 'flex',
        flexDirection: 'column',
        gap: '2px'
    },
    dangerLabel: {
        color: tokens.colorPaletteRedForeground1,
        fontWeight: 'semibold',
        fontSize: '11px',
        textTransform: 'uppercase',
        letterSpacing: '0.5px'
    },
    successLabel: {
        color: tokens.colorPaletteGreenForeground1,
        fontWeight: 'semibold',
        fontSize: '11px',
        textTransform: 'uppercase',
        letterSpacing: '0.5px'
    },
    headerBadgeRow: {
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
        flexWrap: 'wrap',
        marginBottom: '4px',
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
        <SpfxModal
            isOpen={isOpen}
            onClose={onClose}
            title={request.EventName || 'Request Details'}
            width={860}
            footer={
                <Button appearance="secondary" onClick={onClose}>Close Details</Button>
            }
        >
            {/* Status badge row */}
            <div className={styles.headerBadgeRow}>
                <StatusBadge status={request.Status} />
                <Text size={200} style={{ color: '#616161' }}>
                    Submitted by {request.SubmitterName} ({request.SubmitterEmail})
                </Text>
            </div>

            <Divider style={{ margin: '12px 0' }} />

            <div className={styles.content}>

                {/* SECTION 1: Event Details */}
                <div className={styles.section}>
                    <Text weight="bold" size={400}>1. Event Details</Text>
                    <div className={styles.row}>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Event Name</Text>
                            <Text className={styles.value}>{request.EventName || 'N/A'}</Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Location</Text>
                            <Text className={styles.value}>{request.EventLocation || 'N/A'}</Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Start Date</Text>
                            <Text className={styles.value}>
                                {request.EventStartDate ? new Date(request.EventStartDate).toLocaleDateString() : 'N/A'}
                            </Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>End Date</Text>
                            <Text className={styles.value}>
                                {request.EventEndDate ? new Date(request.EventEndDate).toLocaleDateString() : 'N/A'}
                            </Text>
                        </div>
                    </div>
                    <div className={styles.fieldGroup}>
                        <Text className={styles.label}>Attendees</Text>
                        <Text className={styles.value}>{request.Attendees || 'N/A'}</Text>
                    </div>
                    <div className={styles.fieldGroup}>
                        <Text className={styles.label}>How Many Attended</Text>
                        <Text className={styles.value}>{request.HowManyAttended?.toString() || 'N/A'}</Text>
                    </div>
                </div>

                {/* SECTION 2: Corporate Priorities */}
                <div className={styles.section}>
                    <Text weight="bold" size={400}>2. Corporate Priorities Alignment</Text>
                    <div className={styles.fieldGroup}>
                        <Text className={styles.label}>Primary Objective</Text>
                        <Text className={styles.value}>{request.PrimaryObjective || 'N/A'}</Text>
                    </div>
                    <div className={styles.fieldGroup}>
                        <Text className={styles.label}>Corporate Priorities Supported</Text>
                        {priorities.length > 0 ? (
                            <ul className={styles.priorityList}>
                                {priorities.map(p => <li key={p} className={styles.value}>{p}</li>)}
                            </ul>
                        ) : (
                            <Text className={styles.value}>N/A</Text>
                        )}
                    </div>
                    <div className={styles.fieldGroup}>
                        <Text className={styles.label}>Knowledge Sharing Plan</Text>
                        <Text className={styles.value}>{request.KnowledgeSharingPlan || 'N/A'}</Text>
                    </div>
                    <div className={styles.fieldGroup}>
                        <Text className={styles.label}>Previously Attended</Text>
                        <Text className={styles.value}>{request.PreviouslyAttended || 'No'}</Text>
                    </div>
                </div>

                {/* SECTION 3: Costs */}
                <div className={styles.section}>
                    <Text weight="bold" size={400}>3. Total Expense / Cost Allocation</Text>
                    <div className={styles.row}>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Registration Cost</Text>
                            <Text className={styles.value}>${request.RegistrationCost?.toFixed(2) || '0.00'}</Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Airfare Cost</Text>
                            <Text className={styles.value}>${request.AirfareCost?.toFixed(2) || '0.00'}</Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Lodging Cost</Text>
                            <Text className={styles.value}>${request.LodgingCost?.toFixed(2) || '0.00'}</Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Meeting Room Rental</Text>
                            <Text className={styles.value}>${request.MeetingRoomRentalCost?.toFixed(2) || '0.00'}</Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Car Rental Cost</Text>
                            <Text className={styles.value}>${request.CarRentalCost?.toFixed(2) || '0.00'}</Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Travel Meal Allowance</Text>
                            <Text className={styles.value}>${request.TravelMealAllowanceCost?.toFixed(2) || '0.00'}</Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Conference Meals</Text>
                            <Text className={styles.value}>${request.ConferenceMealsCost?.toFixed(2) || '0.00'}</Text>
                        </div>
                        <div className={styles.fieldGroup}>
                            <Text className={styles.label}>Other Cost</Text>
                            <Text className={styles.value}>${request.OtherCost?.toFixed(2) || '0.00'}</Text>
                        </div>
                    </div>
                    <div className={styles.totalSection}>
                        <Text weight="bold">TOTAL ESTIMATED BUDGET</Text>
                        <Text weight="bold" size={500}>${request.TotalEstimatedBudget?.toFixed(2) || '0.00'}</Text>
                    </div>
                    {request.AdditionalComments && (
                        <div className={styles.fieldGroup} style={{ marginTop: '8px' }}>
                            <Text className={styles.label}>Additional Comments</Text>
                            <Text className={styles.value}>{request.AdditionalComments}</Text>
                        </div>
                    )}
                </div>

                {/* SECTION 4: Approval Trail */}
                {(request.GLCode || request.ManagerDenialReason || request.OrgDevDenialReason || request.AccountingDenialReason) && (
                    <div className={styles.section} style={{ gap: '16px' }}>
                        <Text weight="bold" size={400}>4. Approval Notes</Text>

                        {request.GLCode && (
                            <div className={styles.fieldGroup}>
                                <Text className={styles.successLabel}>GL Code Assigned</Text>
                                <Text className={styles.value}>{request.GLCode}</Text>
                            </div>
                        )}
                        {request.ManagerDenialReason && (
                            <div className={styles.fieldGroup}>
                                <Text className={styles.dangerLabel}>Manager Denial Reason</Text>
                                <Text className={styles.value}>{request.ManagerDenialReason}</Text>
                            </div>
                        )}
                        {request.OrgDevDenialReason && (
                            <div className={styles.fieldGroup}>
                                <Text className={styles.dangerLabel}>Org Dev Denial Reason</Text>
                                <Text className={styles.value}>{request.OrgDevDenialReason}</Text>
                            </div>
                        )}
                        {request.AccountingDenialReason && (
                            <div className={styles.fieldGroup}>
                                <Text className={styles.dangerLabel}>Accounting Denial Reason</Text>
                                <Text className={styles.value}>{request.AccountingDenialReason}</Text>
                            </div>
                        )}
                    </div>
                )}
            </div>
        </SpfxModal>
    );
};
