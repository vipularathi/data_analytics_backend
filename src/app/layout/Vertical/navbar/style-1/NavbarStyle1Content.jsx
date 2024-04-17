import { styled } from "@mui/material";
import clsx from "clsx";
import { observer } from "mobx-react-lite";
import Logo from "../../../shared-components/Logo";
import NavbarToggleButton from "../../../shared-components/NavbarToggleButton";
import Navigation from "../../../shared-components/Navigation";

const Root = styled("div")(({ theme }) => ({
  backgroundColor: theme.palette.background.default,
  color: theme.palette.text.primary,
  "& ::-webkit-scrollbar-thumb": {
    boxShadow: `inset 0 0 0 20px ${theme.palette.mode === "light" ? "rgba(0, 0, 0, 0.24)" : "rgba(255, 255, 255, 0.24)"}`,
  },
  "& ::-webkit-scrollbar-thumb:active": {
    boxShadow: `inset 0 0 0 20px ${theme.palette.mode === "light" ? "rgba(0, 0, 0, 0.37)" : "rgba(255, 255, 255, 0.37)"}`,
  },
}));

const StyledContent = styled("div")(() => ({
  overscrollBehavior: "contain",
  overflowX: "hidden",
  overflowY: "auto",
  WebkitOverflowScrolling: "touch",
  background:
    "linear-gradient(rgba(0, 0, 0, 0) 30%, rgba(0, 0, 0, 0) 30%), linear-gradient(rgba(0, 0, 0, 0.25) 0, rgba(0, 0, 0, 0) 40%)",
  backgroundRepeat: "no-repeat",
  backgroundSize: "100% 40px, 100% 10px",
  backgroundAttachment: "local, scroll",
}));
const NavbarStyle1Content = observer(({ className }) => {
  return (
    <Root
      className={clsx(
        "flex h-full flex-auto flex-col overflow-hidden",
        className
      )}
    >
      <div className="flex h-48 shrink-0 flex-row items-center px-12 md:h-76">
        <div className="mx-4 flex flex-1">
          <Logo />
        </div>

        <NavbarToggleButton className="h-40 w-40 p-0" />
      </div>

      <StyledContent
        option={{ suppressScrollX: true, wheelPropagation: false }}
      >
        <Navigation layout="vertical" />
      </StyledContent>
    </Root>
  );
});

export default NavbarStyle1Content;
