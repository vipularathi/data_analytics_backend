import { observer } from "mobx-react-lite";
import navigationConfig from "../../config/navigationConfig";
import { List, styled } from "@mui/material";
import clsx from "clsx";
import NavItem from "./NavItem";
import { useRouter } from "@tanstack/react-router";

const StyledList = styled(List)(({ theme }) => ({
  "& .fuse-list-item": {
    "&:hover": {
      backgroundColor:
        theme.palette.mode === "dark"
          ? "rgba(255, 255, 255, 0.05)"
          : "rgba(0,0,0,.04)",
    },
    "&:focus:not(.active)": {
      backgroundColor:
        theme.palette.mode === "dark"
          ? "rgba(255, 255, 255, 0.06)"
          : "rgba(0,0,0,.05)",
    },
  },
  "& .fuse-list-item-text": {
    margin: 0,
  },
  "& .fuse-list-item-text-primary": {
    lineHeight: "20px",
  },
  "&.active-square-list": {
    "& .fuse-list-item, & .active.fuse-list-item": {
      width: "100%",
      borderRadius: "0",
    },
  },
  "&.dense": {
    "& .fuse-list-item": {
      paddingTop: 0,
      paddingBottom: 0,
      height: 32,
    },
  },
}));
const Navigation = ({ className }) => {
  return (
    <StyledList className={clsx("navigation whitespace-nowrap px-12 py-0")}>
      {navigationConfig.map((_item) => (
        <NavItem key={_item.id} item={_item} />
      ))}
    </StyledList>
  );
};

export default Navigation;