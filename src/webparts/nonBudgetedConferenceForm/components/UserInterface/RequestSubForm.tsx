import * as React from 'react';
import { useState } from 'react';
import {
    makeStyles,
    Button,
    Input,
    Textarea,
    Checkbox,
    Label,
    Text,
    Spinner,
    shorthands,
    tokens
} from '@fluentui/react-components';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';
import { CORPORATE_PRIORITIES } from '../../../../constants';
import { useAppContext } from '../../../../context/AppContext';

const useStyles = makeStyles({
    formContainer: {
        display: 'flex',
        flexDirection: 'column',
        gap: '24px'
    },
    section: {
        display: 'flex',
        flexDirection: 'column',
        gap: '16px',
        backgroundColor: tokens.colorNeutralBackground1,
        ...shorthands.padding('24px'),
        ...shorthands.borderRadius('8px'),
        boxShadow: tokens.shadow2,
        border: `1px solid ${tokens.colorNeutralStroke1}`
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
    errorText: {
        color: tokens.colorPaletteRedForeground1,
        fontSize: '12px'
    },
    totalSection: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        backgroundColor: tokens.colorNeutralBackground3,
        ...shorthands.padding('16px'),
        ...shorthands.borderRadius('4px')
    },
    actions: {
        display: 'flex',
        gap: '12px',
        justifyContent: 'flex-end',
        marginTop: '16px'
    }
});

interface IRequestSubFormProps {
    onSubmitSuccess: () => void;
    draftData?: IConferenceRequest;
}

export const RequestSubForm: React.FC<IRequestSubFormProps> = ({ onSubmitSuccess, draftData }) => {
    const styles = useStyles();
    const { spService, currentUser, graphService } = useAppContext();

    const [loading, setLoading] = useState(false);
    const [errorMsg, setErrorMsg] = useState('');

    const [formData, setFormData] = useState<Partial<IConferenceRequest>>({
        EventName: '',
        EventLocation: '',
        Attendees: '',
        PrimaryObjective: '',
        CorporatePriorities: '[]',
        KnowledgeSharingPlan: '',
        PreviouslyAttended: '',
        HowManyAttended: 1,
        RegistrationCost: 0,
        AirfareCost: 0,
        LodgingCost: 0,
        MeetingRoomRentalCost: 0,
        CarRentalCost: 0,
        TravelMealAllowanceCost: 0,
        ConferenceMealsCost: 0,
        OtherCost: 0,
        AdditionalComments: '',
        Status: 'Draft'
    });

    const [startDate, setStartDate] = useState<string>('');
    const [endDate, setEndDate] = useState<string>('');
    const [selectedPriorities, setSelectedPriorities] = useState<string[]>([]);
    const [attachments, setAttachments] = useState<File[]>([]);

    React.useEffect(() => {
        if (draftData) {
            setFormData({
                ...draftData
            });
            if (draftData.EventStartDate) {
                setStartDate(draftData.EventStartDate.split('T')[0]);
            }
            if (draftData.EventEndDate) {
                setEndDate(draftData.EventEndDate.split('T')[0]);
            }
            if (draftData.CorporatePriorities) {
                try {
                    const parsed = JSON.parse(draftData.CorporatePriorities);
                    setSelectedPriorities(Array.isArray(parsed) ? parsed : []);
                } catch (e) {
                    setSelectedPriorities([]);
                }
            }
        }
    }, [draftData]);

    const handleInputChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
        const { name, value } = e.target;
        setFormData(prev => ({ ...prev, [name]: value }));
    };

    const handleCostChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const { name, value } = e.target;
        const numValue = parseFloat(value) || 0;
        setFormData(prev => ({ ...prev, [name]: numValue }));
    };

    const handlePriorityChange = (priority: string, checked: boolean) => {
        setSelectedPriorities(prev => {
            const next = checked ? [...prev, priority] : prev.filter(p => p !== priority);
            setFormData(f => ({ ...f, CorporatePriorities: JSON.stringify(next) }));
            return next;
        });
    };

    const calculateTotal = () => {
        return (
            (formData.RegistrationCost || 0) +
            (formData.AirfareCost || 0) +
            (formData.LodgingCost || 0) +
            (formData.MeetingRoomRentalCost || 0) +
            (formData.CarRentalCost || 0) +
            (formData.TravelMealAllowanceCost || 0) +
            (formData.ConferenceMealsCost || 0) +
            (formData.OtherCost || 0)
        );
    };

    const validateForm = (): boolean => {
        if (!formData.EventName || !formData.EventLocation || !formData.Attendees ||
            !startDate || !endDate || !formData.PrimaryObjective ||
            selectedPriorities.length === 0 || !formData.KnowledgeSharingPlan ||
            !formData.HowManyAttended) {
            setErrorMsg('Please fill in all required fields.');
            return false;
        }
        setErrorMsg('');
        return true;
    };

    const submitForm = async (status: 'Draft' | 'Pending Manager Approval') => {
        if (status === 'Pending Manager Approval' && !validateForm()) {
            return;
        }

        setLoading(true);
        setErrorMsg('');

        try {
            // Get manager info
            const manager = await graphService.getMyManager();

            const payload: Partial<IConferenceRequest> = {
                ...formData,
                Title: formData.EventName,
                EventStartDate: startDate ? new Date(startDate).toISOString() : null,
                EventEndDate: endDate ? new Date(endDate).toISOString() : null,
                TotalEstimatedBudget: calculateTotal(),
                Status: status,
                SubmitterEmail: currentUser.email,
                SubmitterName: currentUser.displayName,
                ManagerEmail: manager?.email || '',
                SubmittedDate: status === 'Pending Manager Approval' ? new Date().toISOString() : undefined
            };

            let reqId = draftData?.Id;

            if (draftData && draftData.Id) {
                await spService.updateRequest(draftData.Id, payload);
            } else {
                const newReq = await spService.createRequest(payload);
                reqId = newReq.Id;
            }

            if (reqId && attachments.length > 0) {
                for (const file of attachments) {
                    await spService.addAttachment(reqId, file);
                }
            }

            onSubmitSuccess();

        } catch (err) {
            console.error('Submit error:', err);
            setErrorMsg('An error occurred while saving the request.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className={styles.formContainer}>
            {/* SECTION 1: Event Details */}
            <div className={styles.section}>
                <Text weight="semibold" size={500}>1. Event Details</Text>

                <div className={styles.row}>
                    <div className={styles.fieldGroup}>
                        <Label required>Event Name</Label>
                        <Input name="EventName" value={formData.EventName} onChange={handleInputChange} />
                    </div>
                    <div className={styles.fieldGroup}>
                        <Label required>Location (City & State)</Label>
                        <Input name="EventLocation" value={formData.EventLocation} onChange={handleInputChange} />
                    </div>
                </div>

                <div className={styles.row}>
                    <div className={styles.fieldGroup}>
                        <Label required>Start Date</Label>
                        <Input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} />
                    </div>
                    <div className={styles.fieldGroup}>
                        <Label required>End Date</Label>
                        <Input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} />
                    </div>
                </div>

                <div className={styles.fieldGroup}>
                    <Label required>Names of Attendees (Emails)</Label>
                    <Input name="Attendees" value={formData.Attendees} onChange={handleInputChange} placeholder="e.g. john@contoso.com, jane@contoso.com" />
                </div>

                <div className={styles.fieldGroup}>
                    <Label required>How many attended?</Label>
                    <Input type="number" name="HowManyAttended" value={formData.HowManyAttended?.toString()} onChange={handleInputChange} />
                </div>
            </div>

            {/* SECTION 2: Corporate Priorities */}
            <div className={styles.section}>
                <Text weight="semibold" size={500}>2. Corporate Priorities Alignment</Text>

                <div className={styles.fieldGroup}>
                    <Label required>Primary Objective(s) for Attendance</Label>
                    <Textarea name="PrimaryObjective" value={formData.PrimaryObjective} onChange={handleInputChange} />
                </div>

                <div className={styles.fieldGroup}>
                    <Label required>Corporate Priorities Supported</Label>
                    {CORPORATE_PRIORITIES.map(priority => (
                        <Checkbox
                            key={priority}
                            label={priority}
                            checked={selectedPriorities.indexOf(priority) > -1}
                            onChange={(e, data) => handlePriorityChange(priority, !!data.checked)}
                        />
                    ))}
                </div>

                <div className={styles.fieldGroup}>
                    <Label required>Knowledge Sharing Plan [ex: How will you share conference insights with your team and/or organization (e.g., presentations, timeline, meetings)?]</Label>
                    <Textarea name="KnowledgeSharingPlan" value={formData.KnowledgeSharingPlan} onChange={handleInputChange} />
                </div>

                <div className={styles.fieldGroup}>
                    <Label>Previously Attended? (Year(s) and Employee(s))</Label>
                    <Textarea name="PreviouslyAttended" value={formData.PreviouslyAttended} onChange={handleInputChange} />
                </div>
            </div>

            {/* SECTION 3: Costs */}
            <div className={styles.section}>
                <Text weight="semibold" size={500}>3. Total Expense / Cost Allocation</Text>

                <div className={styles.row}>
                    <div className={styles.fieldGroup}>
                        <Label required>Registration Cost</Label>
                        <Input type="number" step="0.01" min="0" name="RegistrationCost" value={formData.RegistrationCost?.toString()} onChange={handleCostChange} contentBefore="$" />
                    </div>
                    <div className={styles.fieldGroup}>
                        <Label required>Airfare Cost</Label>
                        <Input type="number" step="0.01" min="0" name="AirfareCost" value={formData.AirfareCost?.toString()} onChange={handleCostChange} contentBefore="$" />
                    </div>
                    <div className={styles.fieldGroup}>
                        <Label required>Lodging Cost</Label>
                        <Input type="number" step="0.01" min="0" name="LodgingCost" value={formData.LodgingCost?.toString()} onChange={handleCostChange} contentBefore="$" />
                    </div>
                    <div className={styles.fieldGroup}>
                        <Label required>Meeting Room Rental Cost</Label>
                        <Input type="number" step="0.01" min="0" name="MeetingRoomRentalCost" value={formData.MeetingRoomRentalCost?.toString()} onChange={handleCostChange} contentBefore="$" />
                    </div>
                    <div className={styles.fieldGroup}>
                        <Label required>Car Rental Cost</Label>
                        <Input type="number" step="0.01" min="0" name="CarRentalCost" value={formData.CarRentalCost?.toString()} onChange={handleCostChange} contentBefore="$" />
                    </div>
                    <div className={styles.fieldGroup}>
                        <Label required>Travel Meal Allowance</Label>
                        <Input type="number" step="0.01" min="0" name="TravelMealAllowanceCost" value={formData.TravelMealAllowanceCost?.toString()} onChange={handleCostChange} contentBefore="$" />
                    </div>
                    <div className={styles.fieldGroup}>
                        <Label required>Conference Meals Cost</Label>
                        <Input type="number" step="0.01" min="0" name="ConferenceMealsCost" value={formData.ConferenceMealsCost?.toString()} onChange={handleCostChange} contentBefore="$" />
                    </div>
                    <div className={styles.fieldGroup}>
                        <Label required>Other Cost</Label>
                        <Input type="number" step="0.01" min="0" name="OtherCost" value={formData.OtherCost?.toString()} onChange={handleCostChange} contentBefore="$" />
                    </div>
                </div>

                <div className={styles.totalSection}>
                    <Text weight="bold" size={400}>TOTAL ESTIMATED BUDGET:</Text>
                    <Text weight="bold" size={500}>${calculateTotal().toFixed(2)}</Text>
                </div>

                <div className={styles.fieldGroup} style={{ marginTop: '16px' }}>
                    <Label>Additional Comments</Label>
                    <Textarea name="AdditionalComments" value={formData.AdditionalComments} onChange={handleInputChange} />
                </div>
            </div>

            {/* SECTION 4: Attachments */}
            <div className={styles.section}>
                <Text weight="semibold" size={500}>4. Attachments</Text>
                <div className={styles.fieldGroup}>
                    <Label>Attach Files (Agendas, Itineraries, Quotes, etc.)</Label>
                    <input
                        type="file"
                        multiple
                        onChange={(e) => {
                            if (e.target.files) {
                                setAttachments(Array.from(e.target.files));
                            }
                        }}
                    />
                    {attachments.length > 0 && (
                        <div style={{ marginTop: '12px' }}>
                            <Text weight="semibold">Selected Files:</Text>
                            <ul style={{ margin: '4px 0', paddingLeft: '20px' }}>
                                {attachments.map((f, i) => (
                                    <li key={i}>
                                        <Text size={200}>{f.name} ({(f.size / 1024).toFixed(1)} KB)</Text>
                                    </li>
                                ))}
                            </ul>
                            <Button appearance="subtle" size="small" onClick={() => setAttachments([])}>Clear Attachments</Button>
                        </div>
                    )}
                </div>
            </div>

            {errorMsg && <Text className={styles.errorText}>{errorMsg}</Text>}

            <div className={styles.actions}>
                <Button disabled={loading} onClick={() => submitForm('Draft')}>
                    {draftData?.Id ? 'Update Draft' : 'Save as Draft'}
                </Button>
                <Button appearance="primary" disabled={loading} onClick={() => submitForm('Pending Manager Approval')}>
                    {loading ? <Spinner size="tiny" /> : 'Submit for Manager Approval'}
                </Button>
            </div>

            <Text size={200} style={{ color: tokens.colorNeutralForeground3, textAlign: 'right' }}>
                *By approving or submitting this request, I confirm that this event aligns with departmental goals and that budget implications have been considered.
            </Text>
        </div>
    );
};
