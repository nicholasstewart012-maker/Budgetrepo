export type RequestStatus =
    | 'Pending Manager Approval'
    | 'Pending Org Dev Approval'
    | 'Pending Accounting Approval'
    | 'Fully Approved'
    | 'Denied'
    | 'Draft';

export interface IConferenceRequest {
    Id?: number; // SharePoint List ID
    Title: string; // Auto-set to Event Name
    EventStartDate: string | null; // ISO string
    EventEndDate: string | null; // ISO string
    EventName: string;
    EventLocation: string;
    Attendees: string; // JSON array of names/emails
    PrimaryObjective: string;
    CorporatePriorities: string; // JSON array of selected priorities
    KnowledgeSharingPlan: string;
    PreviouslyAttended?: string; // Optional
    HowManyAttended: number; // From handwritten note

    // Costs
    RegistrationCost: number;
    AirfareCost: number;
    LodgingCost: number;
    MeetingRoomRentalCost: number;
    CarRentalCost: number;
    TravelMealAllowanceCost: number;
    ConferenceMealsCost: number;
    OtherCost: number;
    TotalEstimatedBudget: number; // Auto-calculated

    AdditionalComments?: string;
    AttachmentURL?: string; // Link to uploaded document
    Status: RequestStatus;

    // Workflow
    SubmitterEmail: string;
    SubmitterName: string;
    ManagerEmail?: string;
    ManagerApprovalDate?: string;
    ManagerDenialReason?: string;
    OrgDevApproverEmail?: string;
    OrgDevApprovalDate?: string;
    OrgDevDenialReason?: string;
    AccountingApproverEmail?: string;
    AccountingApprovalDate?: string;
    AccountingDenialReason?: string;
    GLCode?: string; // Assigned by Accounting

    SubmittedDate?: string;
    Modified?: string;
}
