import { useState, useRef, useEffect } from "react";
import { Check, ChevronDown, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Checkbox } from "@/components/ui/checkbox";

interface MultiSelectProps {
  options: string[];
  value: string[];
  onChange: (value: string[]) => void;
  placeholder?: string;
  maxDisplay?: number;
  keepOpenAfterChange?: boolean;
  showApplyButton?: boolean;
  onApply?: (value: string[]) => void;
}

export function MultiSelect({
  options,
  value,
  onChange,
  placeholder = "Select items...",
  maxDisplay = 2,
  keepOpenAfterChange = false,
  showApplyButton = false,
  onApply
}: MultiSelectProps) {
  const [open, setOpen] = useState(false);
  const [localValue, setLocalValue] = useState(value);
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Sync local value with prop value when it changes externally
  useEffect(() => {
    setLocalValue(value);

    // Keep dropdown open after reload if keepOpenAfterChange is true and there are selections
    if (keepOpenAfterChange && value.length > 0) {
      setOpen(true);
    }
  }, [value, keepOpenAfterChange]);

  const debouncedOnChange = (newValue: string[]) => {
    setLocalValue(newValue); // Update local state immediately for UI

    // If showApplyButton is true, don't call onChange automatically
    if (showApplyButton) {
      return;
    }

    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }
    timeoutRef.current = setTimeout(() => {
      onChange(newValue); // Call parent onChange after delay
    }, 200); // 200ms delay to allow multiple selections
  };

  const handleSelectAll = () => {
    if (localValue.length === options.length) {
      debouncedOnChange([]);
    } else {
      debouncedOnChange(options);
    }
  };

  const handleToggleOption = (option: string) => {
    if (localValue.includes(option)) {
      debouncedOnChange(localValue.filter(v => v !== option));
    } else {
      debouncedOnChange([...localValue, option]);
    }
  };

  const getDisplayText = () => {
    if (localValue.length === 0) {
      return placeholder;
    } else if (localValue.length <= maxDisplay) {
      return localValue.join(", ");
    } else if (localValue.length === options.length) {
      return "All Selected";
    } else {
      return `${localValue.length} selected`;
    }
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-48 justify-between text-left font-normal"
        >
          <span className="truncate">{getDisplayText()}</span>
          <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-48 p-0" align="start">
        <div className="border-b p-2">
          <div className="flex items-center space-x-2">
            <Checkbox
              id="select-all"
              checked={localValue.length === options.length}
              onCheckedChange={(checked) => {
                handleSelectAll();
              }}
            />
            <label
              htmlFor="select-all"
              className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 cursor-pointer"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                handleSelectAll();
              }}
            >
              Select All
            </label>
          </div>
        </div>
        <div className="max-h-60 overflow-auto">
          {options.map((option) => (
            <div
              key={option}
              className="flex items-center space-x-2 p-2 hover:bg-gray-50 cursor-pointer"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                handleToggleOption(option);
              }}
            >
              <Checkbox
                id={option}
                checked={localValue.includes(option)}
                onCheckedChange={(checked) => {
                  handleToggleOption(option);
                }}
              />
              <label
                htmlFor={option}
                className="text-sm leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 cursor-pointer"
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  handleToggleOption(option);
                }}
              >
                {option}
              </label>
            </div>
          ))}
        </div>
        {showApplyButton && (
          <div className="border-t p-2">
            <Button
              type="button"
              size="sm"
              className="w-full"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                if (onApply) {
                  onApply(localValue);
                }
                setOpen(false);
              }}
            >
              Apply Filter ({localValue.length})
            </Button>
          </div>
        )}
      </PopoverContent>
    </Popover>
  );
}