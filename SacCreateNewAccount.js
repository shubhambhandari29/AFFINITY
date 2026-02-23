import {
  TextField,
  Select,
  MenuItem,
  InputLabel,
  FormControl,
  Grid,
  Button,
  Tab,
  Tabs,
  CircularProgress,
  useTheme,
  Typography,
  Paper,
  Autocomplete,
} from "@mui/material";
import { forwardRef, useEffect, useImperativeHandle, useState } from "react";
import { useForm, Controller, FormProvider } from "react-hook-form";
import TabPanel from "../../../../ui/TabPanel";
import AccountService from "./AccountService";
import NcmTab from "./NcmTab";
import LossRunScheduling from "../LossRunScheduling";
import Notes from "../Notes";
import DeductibleBill from "./DeductibleBill";
import ClaimReviewScheduling from "../ClaimReviewScheduling";
import Shi from "../Shi";
import Modal from "../../../../ui/Modal";
import ViewPolicies from "../view-policies/ViewPolicies";
import ViewAffiliates from "../ViewAffiliates";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import api from "../../../../../api/api";
import Loader from "../../../../ui/Loader";
import { sacAccountFieldPermissions } from "../../../../../field-permissions";
import { useSelector } from "react-redux";
import Swal from "sweetalert2";
import { formatDate } from "../../../../../util";
import useDropdownData from "../../../../../hooks/useDropdownData";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import dayjs from "dayjs";
import ViewAssociatedAccounts from "./ViewAssociatedAccounts";

const defaultValues = {
  AcctStatus: "Active",
  CustomerName: "",
  CustomerNum: "",
  SAC_Contact1: "",
  LossCtlRep1: "",
  DateCreated: new Date().toISOString().split("T")[0],
  RelatedEnt: "",
  DateNotif: "",
  SAC_Contact2: "Jenna Houle",
  LossCtlRep2: "",
  OnBoardDate: "",
  TermDate: "",
  AcctOwner: "",
  RiskSolMgr: "",
  OBMethod: "",
  TermCode: "",
  BranchName: "",
  MarketSegmentation: "",
  AccountNotes: "",
  // Account Service
  ServicesReq: "",
  Exceptions: "",
  HCMAccess: "",
  TotalPrem: "",
  ExceptType: "",
  EffectiveDate: "",
  DiscDate: "",
  BusinessType: "",
  AccomForm: "",
  RenewLetterDt: "",
  ServLevel: "",
  AccomType: "",
  InsuredWebsite: "",
  HCM_LOC_ONLY: "No",
  // ncm
  NCMType: "",
  NCMStatus: "",
  NCMStartDt: "",
  NCMEndDt: "",
  NCMTermReason: "",
  NCMComments: "",
  // Loss Run Scheduling
  AddCommentLossRun: "",
  LossRunDistFreq: "",
  LossRunCheckboxes: [
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
  ],
  // // Loss Run Notes
  LossRunNotes: "",
  // // Deductible Bill
  DeductDistFreq: "",
  DeductNotes: "",
  DueDate: "",
  ReceivedDate: "",
  TotalAmtDue: "",
  DeductCheckboxes: [
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
    { checked: false, lastSendDate: "" },
  ],
  // // Claim Review Scheduling
  AddCommentClaimReview: "",
  ClaimRevDistFreq: "",
  CRThresh: "50000",
  ClaimRevCheckboxes: [
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
  ],
  // Claim Review Notes
  ClaimRevNotes: "",
  // shi
  SHI_Complete: "Yes",
  SHI_Comments: "",
  // Change Notes
  ChangeNotes: "",
};

const SacCreateNewAccount = forwardRef((props, ref) => {
  const methods = useForm({ defaultValues });
  const { control, handleSubmit, reset, setValue, getValues } = methods;
  const [tabIndex, setTabIndex] = useState(0);
  const [viewPolicies, setViewPolicies] = useState(false);
  const [viewAffiliates, setViewAffiliates] = useState(false);
  const [viewAssociatedAcc, setViewAssociatedAcc] = useState(false);
  const { column_name } = useParams();
  const [loading, setLoading] = useState(false);
  const user = useSelector((state) => state.auth.user);
  const allowedFields = sacAccountFieldPermissions[user.role];
  const { pathname, state } = useLocation();
  const from = state?.from || "/pending-items";
  const [submitOrSave, setSubmitOrSave] = useState("");
  const navigate = useNavigate();
  const theme = useTheme();
  const [accountStatus, setAccountStatus] = useState("Active");
  const [haveOtherRelatedAccounts, setHaveOtherRelatedAccounts] = useState("");
  const { data: SAC_Contact1Data, loading: SAC_Contact1Loading } =
    useDropdownData("/dropdowns/SAC_Contact1");
  const { data: SAC_Contact2Data, loading: SAC_Contact2Loading } =
    useDropdownData("/dropdowns/SAC_Contact2");
  const { data: AcctOwnerData, loading: AcctOwnerLoading } = useDropdownData(
    "/dropdowns/AcctOwner",
  );
  const { data: LossCtlRep1Data, loading: LossCtlRep1Loading } =
    useDropdownData("/dropdowns/LossCtlRep1");
  const { data: LossCtlRep2Data, loading: LossCtlRep2Loading } =
    useDropdownData("/dropdowns/LossCtlRep2");
  const { data: RiskSolMgrData, loading: RiskSolMgrLoading } = useDropdownData(
    "/dropdowns/RiskSolMgr",
  );
  const { data: TermCodeData, loading: TermCodeLoading } = useDropdownData(
    "/dropdowns/DNRStatus",
  );
  const { data: BranchNameData, loading: BranchNameLoading } = useDropdownData(
    "/dropdowns/BranchName",
  );
  const { isStepper, formData } = props;
  const disableforDirector =
    user.role === "Director" &&
    getValues("Stage") === "Admin" &&
    getValues("IsSubmitted") === 1;

  const isEnabled = (fieldName) => {
    if (allowedFields === "ALL") return true;
    return allowedFields?.includes(fieldName);
  };

  const tabList = [
    {
      label: "Account Service",
      component: <AccountService isEnabled={isEnabled} />,
    },
    { label: "NT24", component: <NcmTab isEnabled={isEnabled} /> },
    {
      label: "Loss Run Scheduling",
      component: <LossRunScheduling isEnabled={isEnabled} />,
    },
    {
      label: "Loss Run Notes",
      component: (
        <Notes
          name="LossRunNotes"
          label="Loss Run Notes"
          isEnabled={isEnabled}
        />
      ),
    },
    {
      label: "Deductible Bill",
      component: <DeductibleBill isEnabled={isEnabled} />,
    },
    {
      label: "Claim Review Scheduling",
      component: <ClaimReviewScheduling isEnabled={isEnabled} />,
    },
    {
      label: "Claim Review Notes",
      component: (
        <Notes
          name="ClaimRevNotes"
          label="Claim Review Notes"
          isEnabled={isEnabled}
        />
      ),
    },
    {
      label: "SHI",
      component: <Shi isEnabled={isEnabled} />,
    },
    {
      label: "Change Notes",
      component: (
        <Notes name="ChangeNotes" label="Change Notes" isEnabled={isEnabled} />
      ),
    },
  ];

  useImperativeHandle(ref, () => ({
    submit: async (actionType) => {
      let resData = false;
      await handleSubmit(async (data) => {
        const mockEvent = { nativeEvent: { submitter: { name: actionType } } };
        const result = await onSubmit(data, mockEvent);
        resData = result;
      }, onError)();
      return resData;
    },
  }));

  const onSubmit = async (data, e) => {
    const operation = e.nativeEvent.submitter.name;

    //alerts for service level & total permium conflicts
    const totalPrem = data.TotalPrem;
    const serviceLevel = data.ServLevel;

    if (
      (totalPrem !== 0 &&
        (serviceLevel === "Decuctible Billing - Special Accounts" ||
          serviceLevel === "Loss Run" ||
          serviceLevel === "Deductible Billing - Paragon" ||
          serviceLevel === "Inactive")) ||
      (totalPrem < 750000 && serviceLevel?.includes("Comprehensive")) ||
      ((totalPrem < 500000 || totalPrem > 750000) &&
        serviceLevel?.includes("Enhanced")) ||
      ((totalPrem < 250000 || totalPrem > 500000) &&
        serviceLevel?.includes("Essential")) ||
      ((totalPrem < 150000 || totalPrem > 250000) &&
        serviceLevel?.includes("Primary")) ||
      ((totalPrem < 0 || totalPrem > 150000) &&
        serviceLevel?.includes("Exception"))
    ) {
      const res = await Swal.fire({
        title: "Service Level Conflict",
        text: "Total Active Policy Premium is in conflict with Service Level. Do you still want to save?",
        icon: "warning",
        showCancelButton: true,
        confirmButtonText: "Yes",
        cancelButtonText: "No",
        iconColor: theme.palette.warning.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
        cancelButtonColor: theme.palette.error.main,
      });

      if (res.dismiss === Swal.DismissReason.cancel) {
        Swal.fire({
          title: "Save Aborted",
          text: "Save procedure terminated by user.",
          icon: "info",
          confirmButtonText: "Ok",
          iconColor: theme.palette.info.main,
          customClass: {
            confirmButton: "swal-confirm-button",
            cancelButton: "swal-cancel-button",
          },
          buttonsStyling: false,
        });
        return;
      }
    }

    //confirmation alert on submission
    if (operation === "submit") {
      const res = await Swal.fire({
        title: "Confirm Submit",
        text:
          user.role === "Underwriter"
            ? "This will save all the changes to the account, is that what you want to do?"
            : "This will save all the changes & close the account. Is that what you want to do?",
        icon: "warning",
        showCancelButton: true,
        confirmButtonText: "Yes",
        cancelButtonText: "No",
        iconColor: theme.palette.warning.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
        cancelButtonColor: theme.palette.error.main,
      });

      if (res.dismiss === Swal.DismissReason.cancel) return;
    }

    let updatedData =
      operation === "save"
        ? {
            ...data,
            Stage: user.role,
            IsSubmitted:
              pathname !== "/sac-create-new-account" &&
              state?.from !== "/pending-items"
                ? 1
                : 0,
          }
        : { ...data, Stage: user.role, IsSubmitted: 1 };

    const LossRunCheckboxes =
      updatedData.LossRunCheckboxes &&
      updatedData.LossRunCheckboxes.map((item, index) => ({
        CustomerNum: updatedData.CustomerNum,
        MthNum: index + 1,
        RptMth: item.checked,
        CompDate: item.lastSendDate,
        NoClaims: item.NoClaims,
        AdHocReport: item.AdHocReport,
      }));

    const DeductCheckboxes =
      updatedData.DeductCheckboxes &&
      updatedData.DeductCheckboxes.map((item, index) => ({
        CustomerNum: updatedData.CustomerNum,
        MthNum: index + 1,
        RptMth: item.checked,
        CompDate: item.lastSendDate,
      }));

    const ClaimRevCheckboxes =
      updatedData.ClaimRevCheckboxes &&
      updatedData.ClaimRevCheckboxes.map((item, index) => ({
        CustomerNum: updatedData.CustomerNum,
        MthNum: index + 1,
        RptMth: item.checked,
        CompDate: item.lastSendDate,
        RptType: item.reportType,
        DelivMeth: item.deliveryMethod,
        CRNumNarr: +item.narrativesProcessed,
      }));

    delete updatedData.LossRunCheckboxes;
    delete updatedData.DeductCheckboxes;
    delete updatedData.ClaimRevCheckboxes;

    updatedData = {
      ...updatedData,
      TotalPrem: String(updatedData.TotalPrem) && "0.00",
    };

    setSubmitOrSave(operation);

    try {
      //Check if same customer already exists or not, in case of inserts
      if (
        user.role === "Underwriter" &&
        pathname.includes("sac-create-new") &&
        !formData
      ) {
        const resSac = await api.get("/sac_account/", {
          params: { CustomerNum: updatedData.CustomerNum },
        });

        if (resSac.status === 200 && resSac.data?.length > 0) {
          Swal.fire({
            title: "Data Validation Error",
            text: "This Customer Number already exists, duplicate records are not permitted",
            icon: "error",
            confirmButtonText: "OK",
            iconColor: theme.palette.error.main,
            customClass: {
              confirmButton: "swal-confirm-button",
              cancelButton: "swal-cancel-button",
            },
            buttonsStyling: false,
          });
          return;
        }
      }

      //Continue saving if customer number does not exist
      const apiCalls = [api.post("/sac_account/upsert", updatedData)];

      if (LossRunCheckboxes) {
        apiCalls.push(
          api.post("/loss_run_frequency/upsert", LossRunCheckboxes),
        );
      }
      if (DeductCheckboxes) {
        apiCalls.push(
          api.post("/deduct_bill_frequency/upsert", DeductCheckboxes),
        );
      }
      if (ClaimRevCheckboxes) {
        apiCalls.push(
          api.post("/claim_review_frequency/upsert", ClaimRevCheckboxes),
        );
      }

      const responses = await Promise.all(apiCalls);
      const allSuccessful = responses.every((res) => res.status === 200);

      if (!allSuccessful)
        throw new Error("Some error occoured, unable to save the data");

      setSubmitOrSave("");

      const res = await Swal.fire({
        title: "Changes Saved",
        text: "Your changes have been saved",
        icon: "success",
        confirmButtonText: "OK",
        iconColor: theme.palette.success.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
      });

      if (res.isConfirmed || res.dismiss) {
        if (isStepper) return updatedData;
        else if (operation === "submit") {
          reset(defaultValues);
          navigate("/pending-items", { replace: true });
        }
        return true;
      }
    } catch (err) {
      console.error(err);
      Swal.fire({
        title: "Error",
        text: "Some error occoured, unable to save the data",
        icon: "error",
        confirmButtonText: "OK",
        iconColor: theme.palette.error.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
      });
    } finally {
      setSubmitOrSave("");
    }
  };

  const onError = (errors) => {
    const errVals = Object.values(errors);

    if (errVals.length > 0) {
      Swal.fire({
        title: "Data Validation Error",
        text: errVals[0].message,
        icon: "error",
        confirmButtonText: "OK",
        iconColor: theme.palette.error.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
      });
    }
  };

  const handleTabChange = (_, newValue) => setTabIndex(newValue);

  useEffect(() => {
    const fetchData = async (searchByColumn, value) => {
      try {
        setLoading(true);
        const [main, lossRun, deductBill, claimReview] = await Promise.all([
          api.get("/sac_account/", { params: { [searchByColumn]: value } }),
          api.get("/loss_run_frequency/", { params: { CustomerNum: value } }),
          api.get("/deduct_bill_frequency/", {
            params: { CustomerNum: value },
          }),
          api.get("/claim_review_frequency/", {
            params: { CustomerNum: value },
          }),
        ]);

        //replacing all null values with empty string
        const data = Object.fromEntries(
          Object.entries(main.data[0]).map(([k, v]) => [
            k,
            v === null ? "" : v,
          ]),
        );

        let formattedData = {
          ...data,
          DateCreated: formatDate(data.DateCreated),
          DateNotif: formatDate(data.DateNotif),
          OnBoardDate: formatDate(data.OnBoardDate),
          TermDate: formatDate(data.TermDate),
          EffectiveDate: formatDate(data.EffectiveDate),
          DiscDate: formatDate(data.DiscDate),
          RenewLetterDt: formatDate(data.RenewLetterDt),
          NCMStartDt: formatDate(data.NCMStartDt),
          NCMEndDt: formatDate(data.NCMEndDt),
        };

        let LossRunCheckboxes = [];
        let DeductCheckboxes = [];
        let ClaimRevCheckboxes = [];

        for (let i = 1; i <= 12; i++) {
          LossRunCheckboxes.push({
            checked: false,
            lastSendDate: "",
            NoClaims: false,
            AdHocReport: false,
          });
          DeductCheckboxes.push({ checked: false, lastSendDate: "" });
          ClaimRevCheckboxes.push({
            checked: false,
            lastSendDate: "",
            reportType: "",
            deliveryMethod: "",
            narrativesProcessed: "",
          });
        }

        if (lossRun.data.length > 0) {
          lossRun.data.forEach((element) => {
            LossRunCheckboxes[element.MthNum - 1].checked =
              element.RptMth === 1;
            LossRunCheckboxes[element.MthNum - 1].lastSendDate = formatDate(
              element.CompDate,
            );
            LossRunCheckboxes[element.MthNum - 1].NoClaims = element.NoClaims;
            LossRunCheckboxes[element.MthNum - 1].AdHocReport =
              element.AdHocReport;
          });
          formattedData.LossRunCheckboxes = LossRunCheckboxes;
        }

        if (deductBill.data.length > 0) {
          deductBill.data.forEach((element) => {
            DeductCheckboxes[element.MthNum - 1].checked = element.RptMth === 1;
            DeductCheckboxes[element.MthNum - 1].lastSendDate = formatDate(
              element.CompDate,
            );
          });
          formattedData.DeductCheckboxes = DeductCheckboxes;
        }

        if (claimReview.data.length > 0) {
          claimReview.data.forEach((element) => {
            ClaimRevCheckboxes[element.MthNum - 1].checked =
              element.RptMth === 1;
            ClaimRevCheckboxes[element.MthNum - 1].lastSendDate = formatDate(
              element.CompDate,
            );
            ClaimRevCheckboxes[element.MthNum - 1].reportType = element.RptType;
            ClaimRevCheckboxes[element.MthNum - 1].deliveryMethod =
              element.DelivMeth;
            ClaimRevCheckboxes[element.MthNum - 1].narrativesProcessed =
              element.CRNumNarr;
          });
          formattedData.ClaimRevCheckboxes = ClaimRevCheckboxes;
        }

        setAccountStatus(formattedData.AcctStatus);
        setHaveOtherRelatedAccounts(formattedData.RelatedEnt);
        reset(formattedData);
      } catch (err) {
        console.error(err);
        Swal.fire({
          title: "Error",
          text: "Some error occoured, unable to load data",
          icon: "error",
          confirmButtonText: "OK",
          iconColor: theme.palette.error.main,
          customClass: {
            confirmButton: "swal-confirm-button",
            cancelButton: "swal-cancel-button",
          },
          buttonsStyling: false,
        });
      } finally {
        setLoading(false);
      }
    };

    if (column_name) {
      const [key, value] = column_name.split("=");
      fetchData(key, value);
    }

    if (isStepper && formData) fetchData("CustomerNum", formData.CustomerNum);

    reset(defaultValues);
  }, [column_name, reset, formData, isStepper, theme.palette.error.main]);

  useEffect(() => {
    if (pathname === "/sac-create-new-account") setAccountStatus("Active");
    if (from.includes("policy") && !isStepper) setViewPolicies(true);
  }, [pathname, from, isStepper]);

  const handleBack = async () => {
    const res = await Swal.fire({
      title: "Confirm Request to Cancel",
      text: "Any unsaved changes will be lost, are you sure you want to do this?",
      icon: "warning",
      showCancelButton: true,
      confirmButtonText: "Yes",
      cancelButtonText: "No",
      iconColor: theme.palette.warning.main,
      customClass: {
        confirmButton: "swal-confirm-button",
        cancelButton: "swal-cancel-button",
      },
      buttonsStyling: false,
      cancelButtonColor: theme.palette.error.main,
    });

    if (res.isConfirmed) navigate("/pending-items", { replace: true });
  };

  const handleDistributionUpdate = async (oldVal, val, field) => {
    const newVal = (
      field === "AcctOwner"
        ? AcctOwnerData
        : field === "LossCtlRep1"
          ? LossCtlRep1Data
          : LossCtlRep2Data
    ).find((i) => i[field === "AcctOwner" ? "SACName" : "RepName"] === val);
    if (!newVal) return;

    const data = {
      CustomerNum: getValues("CustomerNum"),
      RecipCat:
        field === "AcctOwner"
          ? newVal.EmpTitle
          : field === "LossCtlRep1"
            ? "Risk Solutions Consultant"
            : "Risk Solutions Rep",
      DistVia: "Email",
      AttnTo: newVal[field === "AcctOwner" ? "SACName" : "RepName"],
      EMailAddress: newVal[field === "AcctOwner" ? "EMailID" : "LCEmail"],
    };

    try {
      await Promise.allSettled([
        api.post("/claim_review_distribution/delete", [
          { CustomerNum: getValues("CustomerNum"), AttnTo: oldVal },
        ]),
        api.post("/loss_run_distribution/delete", [
          { CustomerNum: getValues("CustomerNum"), AttnTo: oldVal },
        ]),
        api.post("/claim_review_distribution/upsert", [data]),
        api.post("/loss_run_distribution/upsert", [data]),
      ]);
    } catch (error) {
      console.error(error);
      Swal.fire({
        title: "Error",
        text: "Some error occoured, unable to add the data to Recipient Lists",
        icon: "error",
        confirmButtonText: "OK",
        iconColor: theme.palette.error.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
      });
    }
  };

  const handleSAC2Change = (val) => {
    setValue("SAC_Contact2", val);
    const newVal = SAC_Contact2Data.find((i) => i.SACName === val);
    setValue("EmpTwoTitle", newVal.EmpTitle);
    setValue("EmpTwoEmail", newVal.EMailID);
    setValue("EmpTwoTel", newVal.TelNum);
  };

  if (loading) return <Loader size={40} height="600px" />;

  return (
    <>
      {pathname !== "/sac-create-new-account" && (
        <>
          {/*Modal for view policies  */}
          <Modal
            open={viewPolicies}
            onClose={() => setViewPolicies(false)}
            maxWidth="lg"
          >
            <ViewPolicies
              CustomerNum={column_name.split("=")[1]}
              CustomerName={getValues("CustomerName")}
              disableforDirector={disableforDirector}
            />
          </Modal>

          {/*Modal for view affiliates  */}
          <Modal
            open={viewAffiliates}
            onClose={() => setViewAffiliates(false)}
            title="Affiliate List Management"
            maxWidth="md"
          >
            <ViewAffiliates
              accountName={getValues("CustomerName")}
              customerNum={getValues("CustomerNum")}
              disableforDirector={disableforDirector}
            />
          </Modal>

          {/*Modal for view associated accounts  */}
          <Modal
            open={viewAssociatedAcc}
            onClose={() => setViewAssociatedAcc(false)}
            title="Associated Accounts Management"
            maxWidth="md"
          >
            <ViewAssociatedAccounts
              accountName={getValues("CustomerName")}
              customerNum={getValues("CustomerNum")}
              disableforDirector={disableforDirector}
            />
          </Modal>
        </>
      )}

      <FormProvider {...methods}>
        <form
          onSubmit={handleSubmit(onSubmit, onError)}
          noValidate
          style={{ height: "100%" }}
        >
          <Grid
            container
            spacing={1.5}
            sx={{ height: "100%", alignContent: "flex-start" }}
          >
            {/* Main Form Container */}
            <Grid size={{ xs: 12 }}>
              <Paper
                sx={{
                  p: 2,
                  backgroundColor: "#EBF0F8",
                  border: "1px solid #e0e0e0",
                }}
              >
                <Grid container spacing={1}>
                  <Grid size={{ xs: 12, sm: 3, md: 3 }}>
                    <FormControl fullWidth>
                      <InputLabel>Account Status</InputLabel>
                      <Controller
                        name="AcctStatus"
                        control={control}
                        render={({ field }) => (
                          <Select
                            {...field}
                            label="Account Status"
                            disabled={!isEnabled("AcctStatus")}
                            value={accountStatus}
                            onChange={(e) => {
                              setAccountStatus(e.target.value);
                              setValue("AcctStatus", e.target.value);
                              if (e.target.value !== "Inactive") {
                                setValue("DateNotif", null);
                                setValue("TermDate", null);
                                setValue("TermCode", "");
                              }
                            }}
                          >
                            <MenuItem value="Active">Active</MenuItem>
                            <MenuItem value="Inactive">Inactive</MenuItem>
                            <MenuItem value="Unique">Unique</MenuItem>
                            <MenuItem value="Under Construction">
                              Under Construction
                            </MenuItem>
                          </Select>
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6, md: 6 }}>
                    <Controller
                      name="CustomerName"
                      control={control}
                      disabled={!isEnabled("CustomerName")}
                      rules={{
                        required:
                          "Customer Name is mandatory and cannot be empty",
                      }}
                      render={({ field, fieldState: { error } }) => (
                        <TextField
                          {...field}
                          fullWidth
                          label="Special Account Name"
                          required
                          error={!!error}
                        />
                      )}
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 3, md: 3 }}>
                    <Controller
                      name="CustomerNum"
                      control={control}
                      rules={{
                        required:
                          "Customer Number is mandatory and cannot be empty",
                        validate: (value) => {
                          if (!value || value.length === 10) return true;
                          return "Customer Number must be 10 characters";
                        },
                      }}
                      render={({ field, fieldState: { error } }) => (
                        <TextField
                          {...field}
                          fullWidth
                          label="Cust #"
                          required
                          error={!!error}
                          disabled={
                            !(
                              user.role === "Underwriter" &&
                              pathname.includes("sac-create-new") &&
                              !formData
                            )
                          }
                          slotProps={{
                            htmlInput: {
                              maxLength: 10,
                            },
                          }}
                          onChange={(e) => {
                            // Replace anything that is NOT a number (0-9) with an empty string
                            field.onChange(
                              e.target.value.replace(/[^0-9]/g, ""),
                            );
                          }}
                        />
                      )}
                    />
                  </Grid>

                  <Grid
                    size={{
                      xs: 12,
                      sm: 6,
                      md:
                        pathname !== "/sac-create-new-account" &&
                        state?.from !== "/pending-items"
                          ? 3
                          : 4,
                    }}
                  >
                    <FormControl fullWidth>
                      <Controller
                        name="SAC_Contact1"
                        control={control}
                        disabled={!isEnabled("SAC_Contact1")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={SAC_Contact1Data || []}
                            loading={SAC_Contact1Loading}
                            getOptionLabel={(option) => option.SACName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.SACName === val?.SACName
                            }
                            value={
                              value
                                ? SAC_Contact1Data.find(
                                    (opt) => opt.SACName === value,
                                  ) || { SACName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.SACName : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="SAC 1"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid
                    size={{
                      xs: 12,
                      sm: 6,
                      md:
                        pathname !== "/sac-create-new-account" &&
                        state?.from !== "/pending-items"
                          ? 3
                          : 4,
                    }}
                  >
                    <FormControl fullWidth>
                      <Controller
                        name="LossCtlRep1"
                        control={control}
                        disabled={!isEnabled("LossCtlRep1")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={LossCtlRep1Data || []}
                            loading={LossCtlRep1Loading}
                            getOptionLabel={(option) => option.RepName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.RepName === val?.RepName
                            }
                            value={
                              value
                                ? LossCtlRep1Data.find(
                                    (opt) => opt.RepName === value,
                                  ) || { RepName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              const selectedvalue = newValue
                                ? newValue.RepName
                                : "";
                              onChange(selectedvalue);
                              handleDistributionUpdate(
                                value,
                                selectedvalue,
                                "LossCtlRep1",
                              );
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Risk Solutions 1"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid
                    size={{
                      xs: 12,
                      sm: 6,
                      md:
                        pathname !== "/sac-create-new-account" &&
                        state?.from !== "/pending-items"
                          ? 1.5
                          : 4,
                    }}
                  >
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                      <Controller
                        name="DateCreated"
                        control={control}
                        render={({
                          field: { onChange, value, ...fieldProps },
                        }) => (
                          <DatePicker
                            {...fieldProps}
                            label="Created Date"
                            value={value ? dayjs(value) : null}
                            disabled={!isEnabled("DateCreated")}
                            onChange={(newValue) => {
                              onChange(
                                newValue ? newValue.format("YYYY-MM-DD") : null,
                              );
                            }}
                          />
                        )}
                      />
                    </LocalizationProvider>
                  </Grid>
                  {pathname !== "/sac-create-new-account" &&
                    state?.from !== "/pending-items" && (
                      <>
                        <Grid
                          size={{
                            xs: 12,
                            sm: 6,
                            md: haveOtherRelatedAccounts === "Yes" ? 2 : 4.5,
                          }}
                        >
                          <FormControl fullWidth>
                            <InputLabel>
                              Are there associated accounts?
                            </InputLabel>
                            <Controller
                              name="RelatedEnt"
                              control={control}
                              disabled={!isEnabled("RelatedEnt")}
                              render={({ field }) => (
                                <Select
                                  {...field}
                                  label="Are there associated accounts?"
                                  value={haveOtherRelatedAccounts}
                                  onChange={(e) => {
                                    setHaveOtherRelatedAccounts(e.target.value);
                                    setValue("RelatedEnt", e.target.value);
                                  }}
                                >
                                  <MenuItem value="Yes">Yes</MenuItem>
                                  <MenuItem value="No">No</MenuItem>
                                </Select>
                              )}
                            />
                          </FormControl>
                        </Grid>
                        {haveOtherRelatedAccounts === "Yes" && (
                          <Grid size={{ xs: 12, sm: 6, md: 2.5 }}>
                            <Button
                              name="RelatedEntBtn"
                              variant="outlined"
                              color="primary"
                              fullWidth
                              disabled={!isEnabled("RelatedEntBtn")}
                              onClick={() => setViewAssociatedAcc(true)}
                            >
                              Associated Accounts
                            </Button>
                          </Grid>
                        )}
                      </>
                    )}

                  <Grid size={{ xs: 12, sm: 6, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="SAC_Contact2"
                        control={control}
                        disabled={!isEnabled("SAC_Contact2")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={SAC_Contact2Data || []}
                            loading={SAC_Contact2Loading}
                            getOptionLabel={(option) => option.SACName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.SACName === val?.SACName
                            }
                            value={
                              value
                                ? SAC_Contact2Data.find(
                                    (opt) => opt.SACName === value,
                                  ) || { SACName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              const selectedValue = newValue
                                ? newValue.SACName
                                : "";
                              onChange(selectedValue);
                              handleSAC2Change(selectedValue);
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="SAC 2"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="LossCtlRep2"
                        control={control}
                        disabled={!isEnabled("LossCtlRep2")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={LossCtlRep2Data || []}
                            loading={LossCtlRep2Loading}
                            getOptionLabel={(option) => option.RepName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.RepName === val?.RepName
                            }
                            value={
                              value
                                ? LossCtlRep2Data.find(
                                    (opt) => opt.RepName === value,
                                  ) || { RepName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              const selectedvalue = newValue
                                ? newValue.RepName
                                : "";
                              onChange(selectedvalue);
                              handleDistributionUpdate(
                                value,
                                selectedvalue,
                                "LossCtlRep2",
                              );
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Risk Solutions 2"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6, md: 3 }}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                      <Controller
                        name="OnBoardDate"
                        control={control}
                        disabled={!isEnabled("OnBoardDate")}
                        rules={{
                          required:
                            "On Board Date is mandatory and cannot be empty",
                        }}
                        render={({
                          field: { onChange, value, ...fieldProps },
                          fieldState: { error },
                        }) => (
                          <DatePicker
                            {...fieldProps}
                            label="On Board Date"
                            value={value ? dayjs(value) : null}
                            onChange={(newValue) => {
                              onChange(
                                newValue ? newValue.format("YYYY-MM-DD") : null,
                              );
                            }}
                            slotProps={{
                              textField: { required: true, error: !!error },
                            }}
                          />
                        )}
                      />
                    </LocalizationProvider>
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6, md: 3 }}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                      <Controller
                        name="DateNotif"
                        control={control}
                        rules={{
                          required:
                            isEnabled("DateNotif") &&
                            accountStatus === "Inactive"
                              ? "Notification date is mandatory and cannot be empty"
                              : false,
                        }}
                        render={({
                          field: { onChange, value, ...fieldProps },
                          fieldState: { error },
                        }) => (
                          <DatePicker
                            {...fieldProps}
                            label="Notification Date"
                            value={value ? dayjs(value) : null}
                            onChange={(newValue) => {
                              onChange(
                                newValue ? newValue.format("YYYY-MM-DD") : null,
                              );
                            }}
                            slotProps={{
                              textField: { required: true, error: !!error },
                            }}
                            disabled={
                              !isEnabled("DateNotif") ||
                              (isEnabled("DateNotif") &&
                                accountStatus !== "Inactive")
                            }
                          />
                        )}
                      />
                    </LocalizationProvider>
                  </Grid>

                  <Grid size={{ xs: 12, sm: 6, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="AcctOwner"
                        control={control}
                        disabled={!isEnabled("AcctOwner")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={AcctOwnerData || []}
                            loading={AcctOwnerLoading}
                            getOptionLabel={(option) => option.SACName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.SACName === val?.SACName
                            }
                            value={
                              value
                                ? AcctOwnerData.find(
                                    (opt) => opt.SACName === value,
                                  ) || { SACName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              const selectedvalue = newValue
                                ? newValue.SACName
                                : "";
                              onChange(selectedvalue);
                              handleDistributionUpdate(
                                value,
                                selectedvalue,
                                "AcctOwner",
                              );
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Acct Owner"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="RiskSolMgr"
                        control={control}
                        disabled={!isEnabled("RiskSolMgr")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={RiskSolMgrData || []}
                            loading={RiskSolMgrLoading}
                            getOptionLabel={(option) => option.RepName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.RepName === val?.RepName
                            }
                            value={
                              value
                                ? RiskSolMgrData.find(
                                    (opt) => opt.RepName === value,
                                  ) || { RepName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.RepName : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Risk Mgr"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6, md: 3 }}>
                    <FormControl fullWidth>
                      <InputLabel>On Board Method</InputLabel>
                      <Controller
                        name="OBMethod"
                        control={control}
                        disabled={!isEnabled("OBMethod")}
                        render={({ field }) => (
                          <Select {...field} label="On Board Method">
                            <MenuItem value="In Person">In Person</MenuItem>
                            <MenuItem value="Remote">Remote</MenuItem>
                            <MenuItem value="BOR Change">BOR Change</MenuItem>
                          </Select>
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6, md: 3 }}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                      <Controller
                        name="TermDate"
                        control={control}
                        rules={{
                          required:
                            isEnabled("TermDate") &&
                            accountStatus === "Inactive"
                              ? "Termination date is mandatory and cannot be empty"
                              : false,
                        }}
                        render={({
                          field: { onChange, value, ...fieldProps },
                          fieldState: { error },
                        }) => (
                          <DatePicker
                            {...fieldProps}
                            label="Termination Date"
                            value={value ? dayjs(value) : null}
                            onChange={(newValue) => {
                              onChange(
                                newValue ? newValue.format("YYYY-MM-DD") : null,
                              );
                            }}
                            slotProps={{
                              textField: { required: true, error: !!error },
                            }}
                            disabled={
                              !isEnabled("TermDate") ||
                              (isEnabled("TermDate") &&
                                accountStatus !== "Inactive")
                            }
                          />
                        )}
                      />
                    </LocalizationProvider>
                  </Grid>

                  <Grid size={{ xs: 12, sm: 12, md: 4 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="BranchName"
                        control={control}
                        disabled={!isEnabled("BranchName")}
                        rules={{
                          required:
                            "Branch Name is mandatory and cannot be empty",
                        }}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                          fieldState: { error },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={BranchNameData || []}
                            loading={BranchNameLoading}
                            getOptionLabel={(option) => option.BranchName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.BranchName === val?.BranchName
                            }
                            value={
                              value
                                ? BranchNameData.find(
                                    (opt) => opt.BranchName === value,
                                  ) || { BranchName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.BranchName : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Branch Name"
                                inputRef={ref}
                                required
                                error={!!error}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, sm: 12, md: 4 }}>
                    <FormControl fullWidth>
                      <InputLabel>Market Segmentation</InputLabel>
                      <Controller
                        name="MarketSegmentation"
                        control={control}
                        disabled={!isEnabled("MarketSegmentation")}
                        render={({ field }) => (
                          <Select {...field} label="Market Segmentation">
                            <MenuItem value="Small Commercial">
                              Small Commercial
                            </MenuItem>
                            <MenuItem value="Middle Market">
                              Middle Market
                            </MenuItem>
                            <MenuItem value="Specialty">Specialty</MenuItem>
                          </Select>
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, sm: 12, md: 4 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="TermCode"
                        control={control}
                        rules={{
                          required:
                            isEnabled("TermCode") &&
                            accountStatus === "Inactive"
                              ? "Termination code is mandatory and cannot be empty"
                              : false,
                        }}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            disabled={
                              !isEnabled("TermCode") ||
                              (isEnabled("TermCode") &&
                                accountStatus !== "Inactive")
                            }
                            options={TermCodeData || []}
                            loading={TermCodeLoading}
                            getOptionLabel={(option) => option.DD_Value || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.DD_Value === val?.DD_Value
                            }
                            value={
                              value
                                ? TermCodeData.find(
                                    (opt) => opt.DD_Value === value,
                                  ) || { DD_Value: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.DD_Value : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Termination Reason"
                                inputRef={ref}
                                required
                                disabled={
                                  !isEnabled("TermCode") ||
                                  (isEnabled("TermCode") &&
                                    accountStatus !== "Inactive")
                                }
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>

                  <Grid size={{ xs: 12 }}>
                    <Controller
                      name="AccountNotes"
                      control={control}
                      disabled={!isEnabled("AccountNotes")}
                      render={({ field }) => (
                        <TextField
                          {...field}
                          fullWidth
                          label="Account Details"
                          multiline
                          rows={4}
                        />
                      )}
                    />
                  </Grid>
                  {haveOtherRelatedAccounts === "Yes" && (
                    <Grid size={{ xs: 12 }} sx={{ textAlign: "center" }}>
                      <Typography variant="body1" color="red">
                        This Customer Account Has Other Related Special Account
                        Entities
                      </Typography>
                    </Grid>
                  )}
                </Grid>
              </Paper>
            </Grid>

            <Grid size={{ xs: 12 }} sx={{ width: "100%" }}>
              <Paper
                sx={{
                  pt: 2,
                  px: 2,
                  border: "1px solid #e0e0e0",
                }}
              >
                <Tabs
                  value={tabIndex}
                  onChange={handleTabChange}
                  variant="scrollable"
                  scrollButtons="auto"
                  aria-label="account tabs"
                >
                  {tabList.map((tab) => (
                    <Tab key={tab.label} label={tab.label} />
                  ))}
                </Tabs>

                {tabList.map((tab, i) => (
                  <TabPanel key={i} value={tabIndex} index={i}>
                    {tab.component}
                  </TabPanel>
                ))}
              </Paper>
            </Grid>

            {!isStepper && (
              <Grid
                sx={{
                  width: "100%",
                  display: "flex",
                  justifyContent: "space-between",
                }}
              >
                <Grid sx={{ display: "flex", gap: 2 }}>
                  {pathname !== "/sac-create-new-account" && (
                    <>
                      <Button
                        variant="outlined"
                        color="primary"
                        onClick={() => setViewPolicies(true)}
                      >
                        View Policies
                      </Button>
                      <Button
                        variant="outlined"
                        color="primary"
                        onClick={() => setViewAffiliates(true)}
                      >
                        View Affiliates
                      </Button>
                    </>
                  )}
                </Grid>
                <Grid
                  sx={{
                    display: "flex",
                    gap: 2,
                  }}
                >
                  <Button
                    type="button"
                    variant="outlined"
                    onClick={handleBack}
                    disabled={submitOrSave !== ""}
                  >
                    Back
                  </Button>
                  <Button
                    type="submit"
                    variant="outlined"
                    color="primary"
                    name="save"
                    disabled={submitOrSave !== "" || disableforDirector}
                  >
                    {submitOrSave === "save" ? (
                      <CircularProgress size={15} color="inherit" />
                    ) : (
                      "Save"
                    )}
                  </Button>
                  <Button
                    type="submit"
                    variant="contained"
                    color="primary"
                    name="submit"
                    disabled={submitOrSave !== "" || disableforDirector}
                  >
                    {submitOrSave === "submit" ? (
                      <CircularProgress size={15} color="inherit" />
                    ) : (
                      "Submit"
                    )}
                  </Button>
                </Grid>
              </Grid>
            )}
          </Grid>
        </form>
      </FormProvider>
    </>
  );
});

export default SacCreateNewAccount;
