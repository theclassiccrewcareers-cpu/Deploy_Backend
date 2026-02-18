declare var bootstrap: any;
declare var Chart: any;
declare var msal: any;
declare var marked: {
    parse: (text: string) => string;
};
declare var flatpickr: any;
declare var moment: any;
declare var showToast: (message: string, type?: 'success' | 'error' | 'info') => void;
declare var gsap: any;
declare var Plotly: any;
declare var webkitSpeechRecognition: any;
declare var marked: any;

interface Window {
    openCreateSectionModal: any;
    openCreateDeptModal: any;
    saveGeneratedQuiz: any;
    generatedQuizData: any;
}
